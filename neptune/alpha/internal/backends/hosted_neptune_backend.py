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
from neptune.alpha.exceptions import UnsupportedClientVersion, ProjectNotFound, FileUploadError, \
    ExperimentUUIDNotFound, MetadataInconsistency, NeptuneException
from neptune.alpha.internal.backends.api_model import ClientConfig, Project, Experiment
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.alpha.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.alpha.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.alpha.internal.backends.utils import with_api_exceptions_handler, verify_host_resolution, \
    create_swagger_client, verify_client_version, update_session_proxies
from neptune.alpha.internal.backends.hosted_file_operations import upload_file_attributes
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import Operation, UploadFile
from neptune.alpha.internal.utils import verify_type
from neptune.alpha.internal.utils.paths import path_to_str
from neptune.alpha.types.value import Value
from neptune.alpha.version import version as neptune_client_version
from neptune.internal.storage.storage_utils import UploadEntry
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

        self.backend_client = create_swagger_client(config_api_url + self.BACKEND_SWAGGER_PATH, self._http_client)
        self._client_config = self._get_client_config(self.backend_client)
        verify_client_version(self._client_config, neptune_client_version)

        if config_api_url != self._client_config.api_url:
            self.backend_client = create_swagger_client(self._client_config.api_url + self.BACKEND_SWAGGER_PATH,
                                                        self._http_client)
        self.leaderboard_client = create_swagger_client(self._client_config.api_url + self.LEADERBOARD_SWAGGER_PATH,
                                                        self._http_client)

        # TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
        self._http_client.authenticator = NeptuneAuthenticator(self._get_auth_tokens(), ssl_verify, proxies)

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
            return Project(uuid.UUID(project.id), project.name, project.organizationName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    @with_api_exceptions_handler
    def create_experiment(self, project_uuid: uuid.UUID) -> Experiment:
        verify_type("project_uuid", project_uuid, uuid.UUID)

        params = {
            "projectIdentifier": str(project_uuid),
            "name": "Untitled",
            "parameters": [],
            "properties": [],
            "tags": [],
        }

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': str(neptune_client_version)
        }

        try:
            experiment = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return Experiment(uuid.UUID(experiment.id), experiment.shortId, project_uuid)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_uuid)

    # TODO: Return errors to OperationProcessor
    def execute_operations(self, experiment_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        errors = []

        operations_preprocessor = OperationsPreprocessor()
        operations_preprocessor.process(operations)
        combined_operations = operations_preprocessor.get_operations()
        errors.extend(operations_preprocessor.get_errors())

        upload_operations, other_operations = [], []
        for op in combined_operations:
            (upload_operations if isinstance(op, UploadFile) else other_operations).append(op)

        if other_operations:
            errors.extend(self._execute_operations(experiment_uuid, other_operations))
        if upload_operations:
            errors.extend(self._upload_files(experiment_uuid, upload_operations))

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
            result = self.leaderboard_client.api.sendOperations(**kwargs).response().result
            return [MetadataInconsistency(err.errorDescription) for err in result]
        except HTTPNotFound:
            raise ExperimentUUIDNotFound(exp_uuid=experiment_uuid)

    # Do not use @with_api_exceptions_handler. It should be used internally.
    def _upload_files(self, experiment_uuid: uuid.UUID, operations: List[UploadFile]) -> List[FileUploadError]:
        def get_destination(op: UploadFile) -> str:
            try:
                ext = op.file_path[op.file_path.rindex("."):]
            except ValueError:
                ext = ""
            return '/'.join(op.path) + ext

        upload_entries = [UploadEntry(op.file_path, get_destination(op)) for op in operations]
        return upload_file_attributes(experiment_uuid=experiment_uuid,
                                      upload_entries=upload_entries,
                                      swagger_client=self.leaderboard_client)

    @with_api_exceptions_handler
    def get_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> Value:
        pass

    @with_api_exceptions_handler
    def _get_client_config(self, backend_client: SwaggerClient) -> ClientConfig:
        config = backend_client.api.getClientConfig(X_Neptune_Api_Token=self.credentials.api_token).response().result

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

    @with_api_exceptions_handler
    def _get_auth_tokens(self) -> dict:
        return self.backend_client.api.exchangeApiToken(
            X_Neptune_Api_Token=self.credentials.api_token
        ).response().result
