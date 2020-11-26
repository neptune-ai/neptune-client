#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import platform
import uuid
from typing import List, Optional, Dict

import urllib3
from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound
from bravado.requests_client import RequestsClient
from packaging import version

from neptune.alpha.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE
from neptune.alpha.exceptions import UnsupportedClientVersion, ProjectNotFound, \
    ExperimentUUIDNotFound, MetadataInconsistency, NeptuneException, ExperimentNotFound, NotAlphaProjectException, \
    InternalClientError
from neptune.alpha.internal.backends.api_model import ClientConfig, Project, Experiment, Attribute, AttributeType
from neptune.alpha.internal.backends.hosted_file_operations import upload_file_attribute, download_file_attribute, \
    upload_file_set_attribute, download_zip
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.alpha.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.alpha.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.alpha.internal.backends.utils import with_api_exceptions_handler, verify_host_resolution, \
    create_swagger_client, verify_client_version, update_session_proxies
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import Operation, UploadFile, UploadFileSet
from neptune.alpha.internal.utils import verify_type
from neptune.alpha.internal.utils.paths import path_to_str
from neptune.alpha.types.atoms import GitRef
from neptune.alpha.types.value import Value
from neptune.alpha.version import version as neptune_client_version
from neptune.oauth import NeptuneAuthenticator


class HostedNeptuneBackend(NeptuneBackend):
    BACKEND_SWAGGER_PATH = "/api/backend/swagger.json"
    LEADERBOARD_SWAGGER_PATH = "/api/leaderboard/swagger.json"

    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials

        ssl_verify = True
        if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = self._create_http_client(ssl_verify, proxies)

        config_api_url = self.credentials.api_url_opt or self.credentials.token_origin_address
        if proxies is None:
            verify_host_resolution(config_api_url)

        token_http_client = self._create_http_client(ssl_verify, proxies)
        token_client = create_swagger_client(config_api_url + self.BACKEND_SWAGGER_PATH,
                                             token_http_client)

        self._client_config = self._get_client_config(token_client)
        verify_client_version(self._client_config, neptune_client_version)

        if config_api_url != self._client_config.api_url:
            token_client = create_swagger_client(self._client_config.api_url + self.BACKEND_SWAGGER_PATH,
                                                 token_http_client)

        self.backend_client = create_swagger_client(self._client_config.api_url + self.BACKEND_SWAGGER_PATH,
                                                    self._http_client)
        self.leaderboard_client = create_swagger_client(self._client_config.api_url + self.LEADERBOARD_SWAGGER_PATH,
                                                        self._http_client)

        # TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
        self._http_client.authenticator = NeptuneAuthenticator(
            self.credentials.api_token,
            token_client,
            ssl_verify,
            proxies)

        user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
            lib_version=neptune_client_version,
            system=platform.platform(),
            python_version=platform.python_version())
        self._http_client.session.headers.update({'User-Agent': user_agent})

    def get_display_address(self) -> str:
        return self._client_config.display_url

    @with_api_exceptions_handler
    def get_project(self, project_id: str) -> Project:
        verify_type("project_id", project_id, str)

        try:
            project = self.backend_client.api.getProject(projectIdentifier=project_id).response().result
            if project.version < 2:
                raise NotAlphaProjectException(project_id)
            return Project(uuid.UUID(project.id), project.name, project.organizationName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id: str):
        try:
            exp = self.leaderboard_client.api.getExperiment(experimentId=experiment_id).response().result
            return Experiment(uuid.UUID(exp.id), exp.shortId, exp.organizationName, exp.projectName)
        except HTTPNotFound:
            raise ExperimentNotFound(experiment_id)

    @with_api_exceptions_handler
    def create_experiment(self, project_uuid: uuid.UUID, git_ref: Optional[GitRef] = None) -> Experiment:
        verify_type("project_uuid", project_uuid, uuid.UUID)

        git_info = {
            "commit": {
                "commitId": git_ref.commit_id,
                "message": git_ref.message,
                "authorName": git_ref.author_name,
                "authorEmail": git_ref.author_email,
                "commitDate": git_ref.commit_date
            },
            "repositoryDirty": git_ref.dirty,
            "currentBranch": git_ref.branch,
            "remotes": git_ref.remotes
        } if git_ref else None

        params = {
            "projectIdentifier": str(project_uuid),
            "cliVersion": str(neptune_client_version),
            "gitInfo": git_info
        }

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': str(neptune_client_version)
        }

        try:
            exp = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return Experiment(uuid.UUID(exp.id), exp.shortId, exp.organizationName, exp.projectName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_uuid)

    # TODO: Return errors to OperationProcessor
    def execute_operations(self, experiment_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        errors = []

        operations_preprocessor = OperationsPreprocessor()
        operations_preprocessor.process(operations)
        errors.extend(operations_preprocessor.get_errors())

        upload_operations, other_operations = [], []
        for op in operations_preprocessor.get_operations():
            (upload_operations if isinstance(op, (UploadFile, UploadFileSet)) else other_operations).append(op)

        if other_operations:
            errors.extend(self._execute_operations(experiment_uuid, other_operations))

        for op in upload_operations:
            if isinstance(op, UploadFile):
                try:
                    upload_file_attribute(
                        swagger_client=self.leaderboard_client,
                        experiment_uuid=experiment_uuid,
                        attribute=path_to_str(op.path),
                        file_path=op.file_path)
                except NeptuneException as e:
                    errors.append(e)
            elif isinstance(op, UploadFileSet):
                try:
                    upload_file_set_attribute(
                        swagger_client=self.leaderboard_client,
                        experiment_uuid=experiment_uuid,
                        attribute=path_to_str(op.path),
                        file_globs=op.file_globs,
                        reset=op.reset)
                except NeptuneException as e:
                    errors.append(e)
            else:
                raise InternalClientError("Upload operation in neither File or FileSet")

        return errors

    @with_api_exceptions_handler
    def _execute_operations(self,
                            experiment_uuid: uuid.UUID,
                            operations: List[Operation]) -> List[MetadataInconsistency]:
        kwargs = {
            'experimentId': str(experiment_uuid),
            'operations': [{
                'path': path_to_str(op.path),
                OperationApiNameVisitor().visit(op): OperationApiObjectConverter().convert(op)
            } for op in operations]
        }
        try:
            result = self.leaderboard_client.api.executeOperations(**kwargs).response().result
            return [MetadataInconsistency(err.errorDescription) for err in result]
        except HTTPNotFound:
            raise ExperimentUUIDNotFound(exp_uuid=experiment_uuid)

    @with_api_exceptions_handler
    def get_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> Value:
        # TODO Implement me
        pass

    @with_api_exceptions_handler
    def get_attributes(self, experiment_uuid: uuid.UUID) -> List[Attribute]:
        params = {
            'experimentId': str(experiment_uuid),
        }
        try:
            experiment = self.leaderboard_client.api.getExperimentAttributes(**params).response().result
            return [Attribute(attr.name, AttributeType(attr.type)) for attr in experiment.attributes]
        except HTTPNotFound:
            raise ExperimentUUIDNotFound(exp_uuid=experiment_uuid)

    def download_file(self, experiment_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                experiment_uuid=experiment_uuid,
                attribute=path_to_str(path),
                destination=destination)
        except HTTPNotFound:
            raise MetadataInconsistency("File attribute {} not found".format(path_to_str(path)))

    def download_file_set(self, experiment_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        download_request = self._get_file_set_download_request(experiment_uuid, path)
        try:
            download_zip(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination)
        except HTTPNotFound:
            raise MetadataInconsistency("File attribute {} not found".format(path_to_str(path)))

    @with_api_exceptions_handler
    def _get_file_set_download_request(self, experiment_uuid: uuid.UUID, path: List[str]):
        params = {
            'experimentId': str(experiment_uuid),
            'attribute': path_to_str(path)
        }
        try:
            return self.leaderboard_client.api.prepareForDownloadFileSetAttributeZip(**params).response().result
        except HTTPNotFound:
            raise MetadataInconsistency("File set attribute {} not found".format(path_to_str(path)))

    @with_api_exceptions_handler
    def _get_client_config(self, backend_client: SwaggerClient) -> ClientConfig:
        config = backend_client.api.getClientConfig(
            X_Neptune_Api_Token=self.credentials.api_token,
            alpha="true"
        ).response().result

        if hasattr(config, "pyLibVersions"):
            min_recommended = getattr(config.pyLibVersions, "minRecommendedVersion", None)
            min_compatible = getattr(config.pyLibVersions, "minCompatibleVersion", None)
            max_compatible = getattr(config.pyLibVersions, "maxCompatibleVersion", None)
        else:
            raise UnsupportedClientVersion(neptune_client_version, max_version="0.4.111")

        return ClientConfig(
            api_url=config.apiUrl,
            display_url=config.applicationUrl,
            min_recommended_version=version.parse(min_recommended) if min_recommended else None,
            min_compatible_version=version.parse(min_compatible) if min_compatible else None,
            max_compatible_version=version.parse(max_compatible) if max_compatible else None
        )

    @staticmethod
    def _create_http_client(ssl_verify: bool, proxies: Dict[str, str]) -> RequestsClient:
        http_client = RequestsClient(ssl_verify=ssl_verify)
        update_session_proxies(http_client.session, proxies)
        return http_client
