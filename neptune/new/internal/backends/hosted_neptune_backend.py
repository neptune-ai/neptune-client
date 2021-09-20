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
import logging
import os
import platform
import re
from typing import List, Optional, Dict, Iterable, Tuple, Any

import click
import urllib3
from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound, HTTPUnprocessableEntity
from bravado.requests_client import RequestsClient
from packaging import version
from simplejson import JSONDecodeError

from neptune.new.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.new.exceptions import (
    ClientHttpError,
    FetchAttributeNotFoundException,
    RunNotFound,
    RunUUIDNotFound,
    InternalClientError,
    MetadataInconsistency,
    NeptuneException,
    NeptuneLegacyProjectException,
    ProjectNotFound,
    ProjectNameCollision,
    NeptuneStorageLimitException,
    UnsupportedClientVersion,
    ArtifactNotFoundException,
    NeptuneFeaturesNotAvailableException,
)
from neptune.new.internal.backends.api_model import (
    ApiRun,
    ArtifactAttribute,
    Attribute,
    AttributeType,
    AttributeWithProperties,
    BoolAttribute,
    ClientConfig,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatPointValue,
    FloatSeriesAttribute,
    FloatSeriesValues,
    ImageSeriesValues,
    IntAttribute,
    LeaderboardEntry,
    Project,
    StringAttribute,
    StringPointValue,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
    Workspace,
)
from neptune.new.internal.backends.hosted_file_operations import (
    download_file_attribute,
    download_file_set_attribute,
    download_image_series_element,
    upload_file_attribute,
    upload_file_set_attribute,
)
from neptune.new.internal.backends.hosted_artifact_operations import (
    track_to_new_artifact,
    track_to_existing_artifact
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.new.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.new.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.new.internal.backends.utils import (
    create_swagger_client,
    update_session_proxies,
    verify_client_version,
    verify_host_resolution,
    with_api_exceptions_handler,
    OptionalFeatures,
    MissingApiClient,
)
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.operation import (
    Operation,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.new.internal.utils import verify_type, base64_decode
from neptune.new.internal.utils.generic_attribute_mapper import map_attribute_result_to_value
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.internal.backends.utils import build_operation_url
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef
from neptune.new.version import version as neptune_client_version
from neptune.oauth import NeptuneAuthenticator

_logger = logging.getLogger(__name__)


class HostedNeptuneBackend(NeptuneBackend):
    BACKEND_SWAGGER_PATH = "/api/backend/swagger.json"
    LEADERBOARD_SWAGGER_PATH = "/api/leaderboard/swagger.json"
    ARTIFACTS_SWAGGER_PATH = "/api/artifacts/swagger.json"

    CONNECT_TIMEOUT = 30  # helps detecting internet connection lost
    REQUEST_TIMEOUT = None

    DEFAULT_REQUEST_KWARGS = {
        '_request_options': {
            "connect_timeout": CONNECT_TIMEOUT,
            "timeout": REQUEST_TIMEOUT,
            "headers": {"X-Neptune-LegacyClient": "false"}
        }
    }

    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials
        self.proxies = proxies
        self.missing_features = []

        ssl_verify = True
        if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = self._create_http_client(ssl_verify, proxies)

        config_api_url = self.credentials.api_url_opt or self.credentials.token_origin_address
        if proxies is None:
            verify_host_resolution(config_api_url)

        self._token_http_client = self._create_http_client(ssl_verify, proxies)
        token_client = create_swagger_client(
            build_operation_url(config_api_url, self.BACKEND_SWAGGER_PATH),
            self._token_http_client
        )

        self._client_config = self._get_client_config(token_client)
        verify_client_version(self._client_config, neptune_client_version)

        if config_api_url != self._client_config.api_url:
            token_client = create_swagger_client(
                build_operation_url(self._client_config.api_url, self.BACKEND_SWAGGER_PATH),
                self._token_http_client
            )

        self.backend_client = create_swagger_client(
            build_operation_url(self._client_config.api_url, self.BACKEND_SWAGGER_PATH),
            self._http_client
        )
        self.leaderboard_client = create_swagger_client(
            build_operation_url(self._client_config.api_url, self.LEADERBOARD_SWAGGER_PATH),
            self._http_client
        )
        try:
            self.artifacts_client = create_swagger_client(
                build_operation_url(self._client_config.api_url, self.ARTIFACTS_SWAGGER_PATH),
                self._http_client
            )
        except JSONDecodeError:
            # thanks for nice error handling, bravado
            self.artifacts_client = MissingApiClient(self)
            self.missing_features.append(OptionalFeatures.ARTIFACTS)

        # TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
        self._authenticator = NeptuneAuthenticator(
            self.credentials.api_token,
            token_client,
            ssl_verify,
            proxies)
        self._http_client.authenticator = self._authenticator

        user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
            lib_version=neptune_client_version,
            system=platform.platform(),
            python_version=platform.python_version())
        self._http_client.session.headers.update({'User-Agent': user_agent})

    def close(self) -> None:
        self._http_client.session.close()
        self._token_http_client.session.close()
        self._authenticator.auth.session.close()

    def verify_feature_available(self, feature_name: str):
        if feature_name in self.missing_features:
            raise NeptuneFeaturesNotAvailableException(self.missing_features)

    def get_display_address(self) -> str:
        return self._client_config.display_url

    def websockets_factory(self, project_id: str, run_id: str) -> Optional[WebsocketsFactory]:
        base_url = re.sub(r'^http', 'ws', self._client_config.api_url)
        return WebsocketsFactory(
            url=build_operation_url(
                base_url,
                f'/api/notifications/v1/runs/{project_id}/{run_id}/signal'
            ),
            session=self._authenticator.auth.session,
            proxies=self.proxies
        )

    @with_api_exceptions_handler
    def get_project(self, project_id: str) -> Project:
        verify_type("project_id", project_id, str)

        project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project_id)
        workspace, name = project_spec['workspace'], project_spec['project']

        try:
            if not workspace:
                available_projects = list(filter(lambda p: p.name == name,
                                                 self.get_available_projects(search_term=name)))

                if len(available_projects) == 1:
                    project = available_projects[0]
                    project_id = f'{project.workspace}/{project.name}'
                elif len(available_projects) > 1:
                    raise ProjectNameCollision(
                        project_id=project_id,
                        available_projects=available_projects
                    )
                else:
                    raise ProjectNotFound(
                        project_id=project_id,
                        available_projects=self.get_available_projects(),
                        available_workspaces=self.get_available_workspaces()
                    )

            response = self.backend_client.api.getProject(
                projectIdentifier=project_id,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response()
            warning = response.metadata.headers.get('X-Server-Warning')
            if warning:
                click.echo(warning)  # TODO print in color once colored exceptions are added
            project = response.result
            project_version = project.version if hasattr(project, 'version') else 1
            if project_version < 2:
                raise NeptuneLegacyProjectException(project_id)
            return Project(project.id, project.name, project.organizationName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id,
                                  available_projects=self.get_available_projects(workspace_id=workspace),
                                  available_workspaces=list() if workspace else self.get_available_workspaces())

    @with_api_exceptions_handler
    def get_available_projects(self,
                               workspace_id: Optional[str] = None,
                               search_term: Optional[str] = None
                               ) -> List[Project]:
        try:
            response = self.backend_client.api.listProjects(
                limit=5,
                organizationIdentifier=workspace_id,
                searchTerm=search_term,
                sortBy=['lastViewed'],
                sortDirection=['descending'],
                userRelation='memberOrHigher',
                **self.DEFAULT_REQUEST_KWARGS,
            ).response()
            warning = response.metadata.headers.get('X-Server-Warning')
            if warning:
                click.echo(warning)  # TODO print in color once colored exceptions are added
            projects = response.result.entries
            return list(map(
                lambda project: Project(project.id, project.name, project.organizationName),
                projects))
        except HTTPNotFound:
            return []

    @with_api_exceptions_handler
    def get_available_workspaces(self) -> List[Workspace]:
        try:
            response = self.backend_client.api.listOrganizations(
                **self.DEFAULT_REQUEST_KWARGS,
            ).response()
            warning = response.metadata.headers.get('X-Server-Warning')
            if warning:
                click.echo(warning)  # TODO print in color once colored exceptions are added
            workspaces = response.result
            return list(map(
                lambda workspace: Workspace(_id=workspace.id, name=workspace.name),
                workspaces))
        except HTTPNotFound:
            return []

    @with_api_exceptions_handler
    def get_run(self, run_id: str):
        try:
            run = self.leaderboard_client.api.getExperiment(
                experimentId=run_id,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result
            return ApiRun(run.id, run.shortId, run.organizationName, run.projectName, run.trashed)
        except HTTPNotFound:
            raise RunNotFound(run_id)

    @with_api_exceptions_handler
    def create_run(self,
                   project_id: str,
                   git_ref: Optional[GitRef] = None,
                   custom_run_id: Optional[str] = None,
                   notebook_id: Optional[str] = None,
                   checkpoint_id: Optional[str] = None
                   ) -> ApiRun:
        verify_type("project_id", project_id, str)

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
            "projectIdentifier": project_id,
            "cliVersion": str(neptune_client_version),
            "gitInfo": git_info,
            "customId": custom_run_id,
        }

        if notebook_id is not None and checkpoint_id is not None:
            params["notebookId"] = notebook_id if notebook_id is not None else None
            params["checkpointId"] = checkpoint_id if checkpoint_id is not None else None

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': str(neptune_client_version),
            **self.DEFAULT_REQUEST_KWARGS,
        }

        try:
            run = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return ApiRun(run.id, run.shortId, run.organizationName, run.projectName, run.trashed)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_id)

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id: str, jupyter_path: str) -> Optional[str]:
        try:
            return self.leaderboard_client.api.createEmptyCheckpoint(
                notebookId=notebook_id,
                checkpoint={
                    "path": jupyter_path
                },
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result.id
        except HTTPNotFound:
            return None

    @with_api_exceptions_handler
    def ping_run(self, run_id: str):
        request_kwargs = {
            "_request_options": {
                "timeout": 10, "connect_timeout": 10,
            }
        }
        try:
            self.leaderboard_client.api.ping(
                experimentId=run_id,
                **request_kwargs,
            ).response().result
        except HTTPNotFound:
            raise RunUUIDNotFound(run_id)

    def execute_operations(self, run_id: str, operations: List[Operation]) -> List[NeptuneException]:
        errors = []

        operations_preprocessor = OperationsPreprocessor()
        operations_preprocessor.process(operations)
        errors.extend(operations_preprocessor.get_errors())

        upload_operations, artifact_operations, other_operations = [], [], []

        for op in operations_preprocessor.get_operations():
            if isinstance(
                    op,
                    (UploadFile, UploadFileContent, UploadFileSet)
            ):
                upload_operations.append(op)
            elif isinstance(op, TrackFilesToArtifact):
                artifact_operations.append(op)
            else:
                other_operations.append(op)

        if artifact_operations:
            self.verify_feature_available(OptionalFeatures.ARTIFACTS)

        # Upload operations should be done first since they are idempotent
        errors.extend(
            self._execute_upload_operations_with_400_retry(
                run_id=run_id,
                upload_operations=upload_operations)
        )

        artifact_operations_errors, assign_artifact_operations = self._execute_artifact_operations(
            run_id=run_id,
            artifact_operations=artifact_operations
        )

        errors.extend(artifact_operations_errors)
        other_operations.extend(assign_artifact_operations)

        if other_operations:
            errors.extend(self._execute_operations(run_id, other_operations))

        return errors

    def _execute_upload_operations(self,
                                   run_id: str,
                                   upload_operations: List[Operation]) -> List[NeptuneException]:
        errors = list()

        for op in upload_operations:
            if isinstance(op, UploadFile):
                error = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    run_id=run_id,
                    attribute=path_to_str(op.path),
                    source=op.file_path,
                    ext=op.ext)
                if error:
                    errors.append(error)
            elif isinstance(op, UploadFileContent):
                error = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    run_id=run_id,
                    attribute=path_to_str(op.path),
                    source=base64_decode(op.file_content),
                    ext=op.ext)
                if error:
                    errors.append(error)
            elif isinstance(op, UploadFileSet):
                error = upload_file_set_attribute(
                    swagger_client=self.leaderboard_client,
                    run_id=run_id,
                    attribute=path_to_str(op.path),
                    file_globs=op.file_globs,
                    reset=op.reset)
                if error:
                    errors.append(error)
            else:
                raise InternalClientError("Upload operation in neither File or FileSet")

        return errors

    def _execute_upload_operations_with_400_retry(
            self,
            run_id: str,
            upload_operations: List[Operation]) -> List[NeptuneException]:
        while True:
            try:
                return self._execute_upload_operations(run_id, upload_operations)
            except ClientHttpError as ex:
                if "Length of stream does not match given range" not in ex.response:
                    raise ex

    @with_api_exceptions_handler
    def _execute_artifact_operations(
            self,
            run_id: str,
            artifact_operations: List[TrackFilesToArtifact]
    ) -> Tuple[List[Optional[NeptuneException]], List[Optional[Operation]]]:
        errors = list()
        assign_operations = list()

        for op in artifact_operations:
            try:
                artifact_hash = self.get_artifact_attribute(run_id, op.path).hash
            except FetchAttributeNotFoundException:
                artifact_hash = None

            try:
                if artifact_hash is None:
                    assign_operation = track_to_new_artifact(
                        swagger_client=self.artifacts_client,
                        project_id=op.project_id,
                        path=op.path,
                        parent_identifier=run_id,
                        entries=op.entries,
                        default_request_params=self.DEFAULT_REQUEST_KWARGS
                    )
                else:
                    assign_operation = track_to_existing_artifact(
                        swagger_client=self.artifacts_client,
                        project_id=op.project_id,
                        path=op.path,
                        artifact_hash=artifact_hash,
                        parent_identifier=run_id,
                        entries=op.entries,
                        default_request_params=self.DEFAULT_REQUEST_KWARGS
                    )

                if assign_operation:
                    assign_operations.append(assign_operation)
            except NeptuneException as error:
                errors.append(error)

        return errors, assign_operations

    @with_api_exceptions_handler
    def _execute_operations(self,
                            run_id: str,
                            operations: List[Operation]) -> List[MetadataInconsistency]:
        kwargs = {
            'experimentId': run_id,
            'operations': [{
                'path': path_to_str(op.path),
                OperationApiNameVisitor().visit(op): OperationApiObjectConverter().convert(op)
            } for op in operations],
            **self.DEFAULT_REQUEST_KWARGS,
        }

        try:
            result = self.leaderboard_client.api.executeOperations(**kwargs).response().result
            return [MetadataInconsistency(err.errorDescription) for err in result]
        except HTTPNotFound as e:
            raise RunUUIDNotFound(run_id=run_id) from e
        except HTTPUnprocessableEntity:
            raise NeptuneStorageLimitException()

    @with_api_exceptions_handler
    def get_attributes(self, run_id: str) -> List[Attribute]:
        def to_attribute(attr) -> Attribute:
            return Attribute(attr.name, AttributeType(attr.type))

        params = {
            'experimentId': run_id,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            run = self.leaderboard_client.api.getExperimentAttributes(**params).response().result

            attribute_type_names = [at.value for at in AttributeType]
            accepted_attributes = [attr for attr in run.attributes if attr.type in attribute_type_names]

            # Notify about ignored attrs
            ignored_attributes = \
                set(attr.type for attr in run.attributes) - set(attr.type for attr in accepted_attributes)
            if ignored_attributes:
                _logger.warning(
                    "Ignored following attributes (unknown type): %s.\n"
                    "Try to upgrade `neptune-client.", ignored_attributes
                )

            return [to_attribute(attr) for attr in accepted_attributes if attr.type in attribute_type_names]
        except HTTPNotFound:
            raise RunUUIDNotFound(run_id=run_id)

    def download_file_series_by_index(self, run_id: str, path: List[str],
                                      index: int, destination: str):
        try:
            download_image_series_element(
                swagger_client=self.leaderboard_client,
                run_id=run_id,
                attribute=path_to_str(path),
                index=index,
                destination=destination
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file(self, run_id: str, path: List[str], destination: Optional[str] = None):
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                run_id=run_id,
                attribute=path_to_str(path),
                destination=destination)
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file_set(self, run_id: str, path: List[str], destination: Optional[str] = None):
        download_request = self._get_file_set_download_request(run_id, path)
        try:
            download_file_set_attribute(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination)
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    @with_api_exceptions_handler
    def get_float_attribute(self, run_id: str, path: List[str]) -> FloatAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatAttribute(**params).response().result
            return FloatAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_int_attribute(self, run_id: str, path: List[str]) -> IntAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getIntAttribute(**params).response().result
            return IntAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_bool_attribute(self, run_id: str, path: List[str]) -> BoolAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getBoolAttribute(**params).response().result
            return BoolAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_file_attribute(self, run_id: str, path: List[str]) -> FileAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFileAttribute(**params).response().result
            return FileAttribute(name=result.name, ext=result.ext, size=result.size)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_attribute(self, run_id: str, path: List[str]) -> StringAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringAttribute(**params).response().result
            return StringAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_datetime_attribute(self, run_id: str, path: List[str]) -> DatetimeAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getDatetimeAttribute(**params).response().result
            return DatetimeAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_artifact_attribute(self, run_id: str, path: List[str]) -> ArtifactAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getArtifactAttribute(**params).response().result
            return ArtifactAttribute(
                hash=result.hash
            )
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        params = {
            'projectIdentifier': project_id,
            'hash': artifact_hash,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.artifacts_client.api.listArtifactFiles(**params).response().result
            return [
                ArtifactFileData.from_dto(a) for a in result.files
            ]
        except HTTPNotFound:
            raise ArtifactNotFoundException(artifact_hash)

    @with_api_exceptions_handler
    def get_float_series_attribute(self, run_id: str, path: List[str]) -> FloatSeriesAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesAttribute(**params).response().result
            return FloatSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_attribute(self, run_id: str, path: List[str]) -> StringSeriesAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesAttribute(**params).response().result
            return StringSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_set_attribute(self, run_id: str, path: List[str]) -> StringSetAttribute:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSetAttribute(**params).response().result
            return StringSetAttribute(set(result.values))
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_image_series_values(self, run_id: str, path: List[str],
                                offset: int, limit: int) -> ImageSeriesValues:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getImageSeriesValues(**params).response().result
            return ImageSeriesValues(result.totalItemCount)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_values(self, run_id: str, path: List[str],
                                 offset: int, limit: int) -> StringSeriesValues:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesValues(**params).response().result
            return StringSeriesValues(result.totalItemCount,
                                      [StringPointValue(v.timestampMillis, v.step, v.value) for v in result.values])
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_values(self, run_id: str, path: List[str],
                                offset: int, limit: int) -> FloatSeriesValues:
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesValues(**params).response().result
            return FloatSeriesValues(result.totalItemCount,
                                     [FloatPointValue(v.timestampMillis, v.step, v.value) for v in result.values])
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def fetch_atom_attribute_values(self, run_id: str, path: List[str]) -> List[Tuple[str, AttributeType, Any]]:
        params = {
            'experimentId': run_id,
        }
        try:
            namespace_prefix = path_to_str(path)
            if namespace_prefix:
                # don't want to catch "ns/attribute/other" while looking for "ns/attr"
                namespace_prefix += "/"
            result = self.leaderboard_client.api.getExperimentAttributes(**params).response().result
            return [
                (attr.name, attr.type, map_attribute_result_to_value(attr))
                for attr in result.attributes if attr.name.startswith(namespace_prefix)
            ]
        except HTTPNotFound:
            raise RunUUIDNotFound(run_id)

    @with_api_exceptions_handler
    def _get_file_set_download_request(self, run_id: str, path: List[str]):
        params = {
            'experimentId': run_id,
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            return self.leaderboard_client.api.prepareForDownloadFileSetAttributeZip(**params).response().result
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def _get_client_config(self, backend_client: SwaggerClient) -> ClientConfig:
        config = backend_client.api.getClientConfig(
            X_Neptune_Api_Token=self.credentials.api_token,
            alpha="true",
            **self.DEFAULT_REQUEST_KWARGS,
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

    @with_api_exceptions_handler
    def get_leaderboard(self, project_id: str,
                        _id: Optional[Iterable[str]] = None,
                        state: Optional[Iterable[str]] = None,
                        owner: Optional[Iterable[str]] = None,
                        tags: Optional[Iterable[str]] = None
                        ) -> List[LeaderboardEntry]:

        def get_portion(limit, offset):
            return self.leaderboard_client.api.getLeaderboard(
                projectIdentifier=project_id,
                shortId=_id, state=state, owner=owner, tags=tags, tagsMode='and',
                sortBy=['shortId'], sortFieldType=['string'], sortDirection=['ascending'],
                limit=limit, offset=offset,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result.entries

        def to_leaderboard_entry(entry) -> LeaderboardEntry:
            supported_attribute_types = {item.value for item in AttributeType}
            attributes: List[AttributeWithProperties] = []
            for attr in entry.attributes:
                if attr.type in supported_attribute_types:
                    properties = attr.__getitem__("{}Properties".format(attr.type))
                    attributes.append(AttributeWithProperties(
                        attr.name,
                        AttributeType(attr.type),
                        properties
                    ))
            return LeaderboardEntry(entry.id, attributes)

        try:
            return [to_leaderboard_entry(e) for e in self._get_all_items(get_portion, step=100)]
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    def get_run_url(self, run_id: str, workspace: str, project_name: str, short_id: str) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/e/{short_id}"

    @staticmethod
    def _create_http_client(ssl_verify: bool, proxies: Dict[str, str]) -> RequestsClient:
        http_client = RequestsClient(ssl_verify=ssl_verify)
        http_client.session.verify = ssl_verify
        update_session_proxies(http_client.session, proxies)
        return http_client

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []
        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items
        return items
