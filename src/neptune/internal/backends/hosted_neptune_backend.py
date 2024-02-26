#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["HostedNeptuneBackend"]

import itertools
import os
import re
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from bravado.exception import (
    HTTPConflict,
    HTTPNotFound,
    HTTPPaymentRequired,
    HTTPUnprocessableEntity,
)

from neptune.api.dtos import FileEntry
from neptune.api.searching_entries import iter_over_pages
from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.exceptions import (
    ClientHttpError,
    InternalClientError,
    NeptuneException,
)
from neptune.common.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.core.components.operation_storage import OperationStorage
from neptune.envs import NEPTUNE_FETCH_TABLE_STEP_SIZE
from neptune.exceptions import (
    AmbiguousProjectName,
    ContainerUUIDNotFound,
    FetchAttributeNotFoundException,
    FileSetNotFound,
    MetadataContainerNotFound,
    MetadataInconsistency,
    NeptuneFeatureNotAvailableException,
    NeptuneLegacyProjectException,
    NeptuneLimitExceedException,
    NeptuneObjectCreationConflict,
    ProjectNotFound,
    ProjectNotFoundWithSuggestions,
)
from neptune.internal.artifacts.types import ArtifactFileData
from neptune.internal.backends.api_model import (
    ApiExperiment,
    ArtifactAttribute,
    Attribute,
    AttributeType,
    BoolAttribute,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatPointValue,
    FloatSeriesAttribute,
    FloatSeriesValues,
    ImageSeriesValues,
    IntAttribute,
    LeaderboardEntry,
    OptionalFeatures,
    Project,
    StringAttribute,
    StringPointValue,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
    Workspace,
)
from neptune.internal.backends.hosted_artifact_operations import (
    get_artifact_attribute,
    list_artifact_files,
    track_to_existing_artifact,
    track_to_new_artifact,
)
from neptune.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    create_artifacts_client,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
)
from neptune.internal.backends.hosted_file_operations import (
    download_file_attribute,
    download_file_set_attribute,
    download_image_series_element,
    upload_file_attribute,
    upload_file_set_attribute,
)
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.internal.backends.utils import (
    ExecuteOperationsBatchingManager,
    MissingApiClient,
    build_operation_url,
    ssl_verify,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.internal.operation import (
    DeleteAttribute,
    Operation,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.internal.utils import base64_decode
from neptune.internal.utils.generic_attribute_mapper import map_attribute_result_to_value
from neptune.internal.utils.git import GitInfo
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.paths import path_to_str
from neptune.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.management.exceptions import ObjectNotFound
from neptune.typing import ProgressBarType
from neptune.version import version as neptune_client_version

if TYPE_CHECKING:
    from bravado.requests_client import RequestsClient

    from neptune.internal.backends.api_model import ClientConfig


_logger = get_logger()

ATOMIC_ATTRIBUTE_TYPES = {
    AttributeType.INT.value,
    AttributeType.FLOAT.value,
    AttributeType.STRING.value,
    AttributeType.BOOL.value,
    AttributeType.DATETIME.value,
    AttributeType.RUN_STATE.value,
}

ATOMIC_ATTRIBUTE_TYPES = {
    AttributeType.INT.value,
    AttributeType.FLOAT.value,
    AttributeType.STRING.value,
    AttributeType.BOOL.value,
    AttributeType.DATETIME.value,
    AttributeType.RUN_STATE.value,
}


class HostedNeptuneBackend(NeptuneBackend):
    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials
        self.proxies = proxies
        self.missing_features = []

        self._http_client, self._client_config = create_http_client_with_auth(
            credentials=credentials, ssl_verify=ssl_verify(), proxies=proxies
        )  # type: (RequestsClient, ClientConfig)

        self.backend_client = create_backend_client(self._client_config, self._http_client)
        self.leaderboard_client = create_leaderboard_client(self._client_config, self._http_client)

        if self._client_config.has_feature(OptionalFeatures.ARTIFACTS):
            self.artifacts_client = create_artifacts_client(self._client_config, self._http_client)
        else:
            # create a stub
            self.artifacts_client = MissingApiClient(OptionalFeatures.ARTIFACTS)

    def verify_feature_available(self, feature_name: str):
        if not self._client_config.has_feature(feature_name):
            raise NeptuneFeatureNotAvailableException(feature_name)

    def get_display_address(self) -> str:
        return self._client_config.display_url

    def websockets_factory(self, project_id: str, run_id: str) -> Optional[WebsocketsFactory]:
        base_url = re.sub(r"^http", "ws", self._client_config.api_url)
        return WebsocketsFactory(
            url=build_operation_url(base_url, f"/api/notifications/v1/runs/{project_id}/{run_id}/signal"),
            session=self._http_client.authenticator.auth.session,
            proxies=self.proxies,
        )

    @with_api_exceptions_handler
    def get_project(self, project_id: QualifiedName) -> Project:
        project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project_id)
        workspace, name = project_spec["workspace"], project_spec["project"]

        try:
            if not workspace:
                available_projects = list(
                    filter(
                        lambda p: p.name == name,
                        self.get_available_projects(search_term=name),
                    )
                )

                if len(available_projects) == 1:
                    project = available_projects[0]
                    project_id = f"{project.workspace}/{project.name}"
                elif len(available_projects) > 1:
                    raise AmbiguousProjectName(project_id=project_id, available_projects=available_projects)
                else:
                    raise ProjectNotFoundWithSuggestions(
                        project_id=project_id,
                        available_projects=self.get_available_projects(),
                        available_workspaces=self.get_available_workspaces(),
                    )

            response = self.backend_client.api.getProject(
                projectIdentifier=project_id,
                **DEFAULT_REQUEST_KWARGS,
            ).response()
            project = response.result
            project_version = project.version if hasattr(project, "version") else 1
            if project_version < 2:
                raise NeptuneLegacyProjectException(project_id)
            return Project(
                id=project.id,
                name=project.name,
                workspace=project.organizationName,
                sys_id=project.projectKey,
            )
        except HTTPNotFound:
            available_workspaces = self.get_available_workspaces()

            if workspace and not list(filter(lambda aw: aw.name == workspace, available_workspaces)):
                # Could not found specified workspace, forces listing all projects
                raise ProjectNotFoundWithSuggestions(
                    project_id=project_id,
                    available_projects=self.get_available_projects(),
                    available_workspaces=available_workspaces,
                )
            else:
                raise ProjectNotFoundWithSuggestions(
                    project_id=project_id,
                    available_projects=self.get_available_projects(workspace_id=workspace),
                )

    @with_api_exceptions_handler
    def get_available_projects(
        self, workspace_id: Optional[str] = None, search_term: Optional[str] = None
    ) -> List[Project]:
        try:
            response = self.backend_client.api.listProjects(
                limit=5,
                organizationIdentifier=workspace_id,
                searchTerm=search_term,
                sortBy=["lastViewed"],
                sortDirection=["descending"],
                userRelation="memberOrHigher",
                **DEFAULT_REQUEST_KWARGS,
            ).response()
            projects = response.result.entries
            return list(
                map(
                    lambda project: Project(
                        id=project.id,
                        name=project.name,
                        workspace=project.organizationName,
                        sys_id=project.projectKey,
                    ),
                    projects,
                )
            )
        except HTTPNotFound:
            return []

    @with_api_exceptions_handler
    def get_available_workspaces(self) -> List[Workspace]:
        try:
            response = self.backend_client.api.listOrganizations(
                **DEFAULT_REQUEST_KWARGS,
            ).response()
            workspaces = response.result
            return list(
                map(
                    lambda workspace: Workspace(id=workspace.id, name=workspace.name),
                    workspaces,
                )
            )
        except HTTPNotFound:
            return []

    @with_api_exceptions_handler
    def get_metadata_container(
        self,
        container_id: Union[UniqueId, QualifiedName],
        expected_container_type: typing.Optional[ContainerType],
    ) -> ApiExperiment:
        try:
            experiment = (
                self.leaderboard_client.api.getExperiment(
                    experimentId=container_id,
                    **DEFAULT_REQUEST_KWARGS,
                )
                .response()
                .result
            )

            if (
                expected_container_type is not None
                and ContainerType.from_api(experiment.type) != expected_container_type
            ):
                raise MetadataContainerNotFound.of_container_type(
                    container_type=expected_container_type, container_id=container_id
                )

            return ApiExperiment.from_experiment(experiment)
        except ObjectNotFound:
            raise MetadataContainerNotFound.of_container_type(
                container_type=expected_container_type, container_id=container_id
            )

    @with_api_exceptions_handler
    def create_run(
        self,
        project_id: UniqueId,
        git_info: Optional[GitInfo] = None,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiExperiment:

        git_info_serialized = (
            {
                "commit": {
                    "commitId": git_info.commit_id,
                    "message": git_info.message,
                    "authorName": git_info.author_name,
                    "authorEmail": git_info.author_email,
                    "commitDate": git_info.commit_date,
                },
                "repositoryDirty": git_info.dirty,
                "currentBranch": git_info.branch,
                "remotes": git_info.remotes,
            }
            if git_info
            else None
        )

        additional_params = {
            "gitInfo": git_info_serialized,
            "customId": custom_run_id,
        }

        if notebook_id is not None and checkpoint_id is not None:
            additional_params["notebookId"] = notebook_id if notebook_id is not None else None
            additional_params["checkpointId"] = checkpoint_id if checkpoint_id is not None else None

        return self._create_experiment(
            project_id=project_id,
            parent_id=project_id,
            container_type=ContainerType.RUN,
            additional_params=additional_params,
        )

    @with_api_exceptions_handler
    def create_model(self, project_id: UniqueId, key: str = "") -> ApiExperiment:
        additional_params = {
            "key": key,
        }

        return self._create_experiment(
            project_id=project_id,
            parent_id=project_id,
            container_type=ContainerType.MODEL,
            additional_params=additional_params,
        )

    @with_api_exceptions_handler
    def create_model_version(self, project_id: UniqueId, model_id: UniqueId) -> ApiExperiment:
        return self._create_experiment(
            project_id=project_id,
            parent_id=model_id,
            container_type=ContainerType.MODEL_VERSION,
        )

    def _create_experiment(
        self,
        project_id: UniqueId,
        parent_id: UniqueId,
        container_type: ContainerType,
        additional_params: Optional[dict] = None,
    ):
        if additional_params is None:
            additional_params = dict()

        params = {
            "projectIdentifier": project_id,
            "parentId": parent_id,
            "type": container_type.to_api(),
            "cliVersion": str(neptune_client_version),
            **additional_params,
        }

        kwargs = {
            "experimentCreationParams": params,
            "X-Neptune-CliVersion": str(neptune_client_version),
            **DEFAULT_REQUEST_KWARGS,
        }

        try:
            experiment = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return ApiExperiment.from_experiment(experiment)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_id)
        except HTTPConflict as e:
            raise NeptuneObjectCreationConflict() from e

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id: str, jupyter_path: str) -> Optional[str]:
        try:
            return (
                self.leaderboard_client.api.createEmptyCheckpoint(
                    notebookId=notebook_id,
                    checkpoint={"path": jupyter_path},
                    **DEFAULT_REQUEST_KWARGS,
                )
                .response()
                .result.id
            )
        except HTTPNotFound:
            return None

    @with_api_exceptions_handler
    def ping(self, container_id: str, container_type: ContainerType):
        request_kwargs = {
            "_request_options": {
                "timeout": 10,
                "connect_timeout": 10,
            }
        }
        try:
            self.leaderboard_client.api.ping(
                experimentId=container_id,
                **request_kwargs,
            ).response().result
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(container_id, container_type) from e

    def execute_operations(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        operations: List[Operation],
        operation_storage: OperationStorage,
    ) -> Tuple[int, List[NeptuneException]]:
        errors = []

        batching_mgr = ExecuteOperationsBatchingManager(self)
        operations_batch = batching_mgr.get_batch(operations)
        errors.extend(operations_batch.errors)
        dropped_count = operations_batch.dropped_operations_count

        operations_preprocessor = OperationsPreprocessor()
        operations_preprocessor.process(operations_batch.operations)

        preprocessed_operations = operations_preprocessor.get_operations()
        errors.extend(preprocessed_operations.errors)

        if preprocessed_operations.artifact_operations:
            self.verify_feature_available(OptionalFeatures.ARTIFACTS)

        # Upload operations should be done first since they are idempotent
        errors.extend(
            self._execute_upload_operations_with_400_retry(
                container_id=container_id,
                container_type=container_type,
                upload_operations=preprocessed_operations.upload_operations,
                operation_storage=operation_storage,
            )
        )

        (artifact_operations_errors, assign_artifact_operations,) = self._execute_artifact_operations(
            container_id=container_id,
            container_type=container_type,
            artifact_operations=preprocessed_operations.artifact_operations,
        )

        errors.extend(artifact_operations_errors)

        errors.extend(
            self._execute_operations(
                container_id,
                container_type,
                operations=assign_artifact_operations + preprocessed_operations.other_operations,
            )
        )

        for op in itertools.chain(
            preprocessed_operations.upload_operations,
            assign_artifact_operations,
            preprocessed_operations.other_operations,
        ):
            op.clean(operation_storage=operation_storage)

        return (
            operations_preprocessor.processed_ops_count + dropped_count,
            errors,
        )

    def _execute_upload_operations(
        self,
        container_id: str,
        container_type: ContainerType,
        upload_operations: List[Operation],
        operation_storage: OperationStorage,
    ) -> List[NeptuneException]:
        errors = list()

        if self._client_config.has_feature(OptionalFeatures.MULTIPART_UPLOAD):
            multipart_config = self._client_config.multipart_config
            # collect delete operations and execute them
            attributes_to_reset = [
                DeleteAttribute(op.path) for op in upload_operations if isinstance(op, UploadFileSet) and op.reset
            ]
            if attributes_to_reset:
                errors.extend(self._execute_operations(container_id, container_type, operations=attributes_to_reset))
        else:
            multipart_config = None

        for op in upload_operations:
            if isinstance(op, UploadFile):
                upload_errors = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    container_id=container_id,
                    attribute=path_to_str(op.path),
                    source=op.get_absolute_path(operation_storage),
                    ext=op.ext,
                    multipart_config=multipart_config,
                )
                if upload_errors:
                    errors.extend(upload_errors)
            elif isinstance(op, UploadFileContent):
                upload_errors = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    container_id=container_id,
                    attribute=path_to_str(op.path),
                    source=base64_decode(op.file_content),
                    ext=op.ext,
                    multipart_config=multipart_config,
                )
                if upload_errors:
                    errors.extend(upload_errors)
            elif isinstance(op, UploadFileSet):
                upload_errors = upload_file_set_attribute(
                    swagger_client=self.leaderboard_client,
                    container_id=container_id,
                    attribute=path_to_str(op.path),
                    file_globs=op.file_globs,
                    reset=op.reset,
                    multipart_config=multipart_config,
                )
                if upload_errors:
                    errors.extend(upload_errors)
            else:
                raise InternalClientError("Upload operation in neither File or FileSet")

        return errors

    def _execute_upload_operations_with_400_retry(
        self,
        container_id: str,
        container_type: ContainerType,
        upload_operations: List[Operation],
        operation_storage: OperationStorage,
    ) -> List[NeptuneException]:
        while True:
            try:
                return self._execute_upload_operations(
                    container_id, container_type, upload_operations, operation_storage
                )
            except ClientHttpError as ex:
                if "Length of stream does not match given range" not in ex.response:
                    raise ex

    @with_api_exceptions_handler
    def _execute_artifact_operations(
        self,
        container_id: str,
        container_type: ContainerType,
        artifact_operations: List[TrackFilesToArtifact],
    ) -> Tuple[List[Optional[NeptuneException]], List[Optional[Operation]]]:
        errors = list()
        assign_operations = list()

        has_hash_exclude_metadata = self._client_config.has_feature(OptionalFeatures.ARTIFACTS_HASH_EXCLUDE_METADATA)
        has_exclude_directories = self._client_config.has_feature(OptionalFeatures.ARTIFACTS_EXCLUDE_DIRECTORY_FILES)

        for op in artifact_operations:
            try:
                artifact_hash = self.get_artifact_attribute(container_id, container_type, op.path).hash
            except FetchAttributeNotFoundException:
                artifact_hash = None

            try:
                if artifact_hash is None:
                    assign_operation = track_to_new_artifact(
                        swagger_client=self.artifacts_client,
                        project_id=op.project_id,
                        path=op.path,
                        parent_identifier=container_id,
                        entries=op.entries,
                        default_request_params=DEFAULT_REQUEST_KWARGS,
                        exclude_directory_files=has_exclude_directories,
                        exclude_metadata_from_hash=has_hash_exclude_metadata,
                    )
                else:
                    assign_operation = track_to_existing_artifact(
                        swagger_client=self.artifacts_client,
                        project_id=op.project_id,
                        path=op.path,
                        artifact_hash=artifact_hash,
                        parent_identifier=container_id,
                        entries=op.entries,
                        default_request_params=DEFAULT_REQUEST_KWARGS,
                        exclude_directory_files=has_exclude_directories,
                    )

                if assign_operation:
                    assign_operations.append(assign_operation)
            except NeptuneException as error:
                errors.append(error)

        return errors, assign_operations

    @with_api_exceptions_handler
    def _execute_operations(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        operations: List[Operation],
    ) -> List[MetadataInconsistency]:
        kwargs = {
            "experimentId": container_id,
            "operations": [
                {
                    "path": path_to_str(op.path),
                    OperationApiNameVisitor().visit(op): OperationApiObjectConverter().convert(op),
                }
                for op in operations
            ],
            **DEFAULT_REQUEST_KWARGS,
        }

        try:
            result = self.leaderboard_client.api.executeOperations(**kwargs).response().result
            return [MetadataInconsistency(err.errorDescription) for err in result]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(container_id, container_type) from e
        except (HTTPPaymentRequired, HTTPUnprocessableEntity) as e:
            raise NeptuneLimitExceedException(reason=e.response.json().get("title", "Unknown reason")) from e

    @with_api_exceptions_handler
    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        def to_attribute(attr) -> Attribute:
            return Attribute(attr.name, AttributeType(attr.type))

        params = {
            "experimentId": container_id,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            experiment = self.leaderboard_client.api.getExperimentAttributes(**params).response().result

            attribute_type_names = [at.value for at in AttributeType]
            accepted_attributes = [attr for attr in experiment.attributes if attr.type in attribute_type_names]

            # Notify about ignored attrs
            ignored_attributes = set(attr.type for attr in experiment.attributes) - set(
                attr.type for attr in accepted_attributes
            )
            if ignored_attributes:
                _logger.warning(
                    "Ignored following attributes (unknown type): %s.\n" "Try to upgrade `neptune`.",
                    ignored_attributes,
                )

            return [to_attribute(attr) for attr in accepted_attributes if attr.type in attribute_type_names]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e

    def download_file_series_by_index(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        index: int,
        destination: str,
        progress_bar: Optional[ProgressBarType],
    ):
        try:
            download_image_series_element(
                swagger_client=self.leaderboard_client,
                container_id=container_id,
                attribute=path_to_str(path),
                index=index,
                destination=destination,
                progress_bar=progress_bar,
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                container_id=container_id,
                attribute=path_to_str(path),
                destination=destination,
                progress_bar=progress_bar,
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file_set(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        download_request = self._get_file_set_download_request(container_id, container_type, path)
        try:
            download_file_set_attribute(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination,
                progress_bar=progress_bar,
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    @with_api_exceptions_handler
    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatAttribute(**params).response().result
            return FloatAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getIntAttribute(**params).response().result
            return IntAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getBoolAttribute(**params).response().result
            return BoolAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_file_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FileAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFileAttribute(**params).response().result
            return FileAttribute(name=result.name, ext=result.ext, size=result.size)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringAttribute(**params).response().result
            return StringAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DatetimeAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getDatetimeAttribute(**params).response().result
            return DatetimeAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactAttribute:
        return get_artifact_attribute(
            swagger_client=self.leaderboard_client,
            parent_identifier=container_id,
            path=path,
            default_request_params=DEFAULT_REQUEST_KWARGS,
        )

    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        return list_artifact_files(
            swagger_client=self.artifacts_client,
            project_id=project_id,
            artifact_hash=artifact_hash,
            default_request_params=DEFAULT_REQUEST_KWARGS,
        )

    @with_api_exceptions_handler
    def list_fileset_files(self, attribute: List[str], container_id: str, path: str) -> List[FileEntry]:
        attribute = path_to_str(attribute)
        try:
            entries = (
                self.leaderboard_client.api.lsFileSetAttribute(
                    attribute=attribute, path=path, experimentId=container_id, **DEFAULT_REQUEST_KWARGS
                )
                .response()
                .result
            )
            return [FileEntry.from_dto(entry) for entry in entries]
        except HTTPNotFound:
            raise FileSetNotFound(attribute, path)

    @with_api_exceptions_handler
    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesAttribute(**params).response().result
            return FloatSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesAttribute(**params).response().result
            return StringSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSetAttribute(**params).response().result
            return StringSetAttribute(set(result.values))
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_image_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> ImageSeriesValues:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            "limit": limit,
            "offset": offset,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getImageSeriesValues(**params).response().result
            return ImageSeriesValues(result.totalItemCount)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> StringSeriesValues:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            "limit": limit,
            "offset": offset,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesValues(**params).response().result
            return StringSeriesValues(
                result.totalItemCount,
                [StringPointValue(v.timestampMillis, v.step, v.value) for v in result.values],
            )
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> FloatSeriesValues:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            "limit": limit,
            "offset": offset,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesValues(**params).response().result
            return FloatSeriesValues(
                result.totalItemCount,
                [FloatPointValue(v.timestampMillis, v.step, v.value) for v in result.values],
            )
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def fetch_atom_attribute_values(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> List[Tuple[str, AttributeType, Any]]:
        params = {
            "experimentId": container_id,
        }
        try:
            namespace_prefix = path_to_str(path)
            if namespace_prefix:
                # don't want to catch "ns/attribute/other" while looking for "ns/attr"
                namespace_prefix += "/"
            result = self.leaderboard_client.api.getExperimentAttributes(**params).response().result
            return [
                (attr.name, attr.type, map_attribute_result_to_value(attr))
                for attr in result.attributes
                if attr.name.startswith(namespace_prefix)
            ]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(container_id, container_type) from e

    @with_api_exceptions_handler
    def _get_file_set_download_request(self, container_id: str, container_type: ContainerType, path: List[str]):
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            return self.leaderboard_client.api.prepareForDownloadFileSetAttributeZip(**params).response().result
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def _get_column_types(self, project_id: UniqueId, column: str, types: Optional[Iterable[str]] = None) -> List[Any]:
        params = {
            "projectIdentifier": project_id,
            "search": column,
            "type": types,
            "params": {},
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            return self.leaderboard_client.api.searchLeaderboardAttributes(**params).response().result.entries
        except HTTPNotFound as e:
            raise ProjectNotFound(project_id=project_id) from e

    @with_api_exceptions_handler
    def search_leaderboard_entries(
        self,
        project_id: UniqueId,
        types: Optional[Iterable[ContainerType]] = None,
        query: Optional[NQLQuery] = None,
        columns: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> Generator[LeaderboardEntry, None, None]:
        default_step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "100"))

        step_size = min(default_step_size, limit) if limit else default_step_size

        types_filter = list(map(lambda container_type: container_type.to_api(), types)) if types else None
        attributes_filter = {"attributeFilters": [{"path": column} for column in columns]} if columns else {}

        if sort_by == "sys/creation_time":
            sort_by_column_type = AttributeType.DATETIME.value
        else:
            sort_by_column_type_candidates = self._get_column_types(project_id, sort_by, types_filter)
            sort_by_column_type = _get_column_type_from_entries(sort_by_column_type_candidates, sort_by)

        try:
            return iter_over_pages(
                client=self.leaderboard_client,
                project_id=project_id,
                types=types_filter,
                query=query,
                attributes_filter=attributes_filter,
                step_size=step_size,
                limit=limit,
                sort_by=sort_by,
                ascending=ascending,
                sort_by_column_type=sort_by_column_type,
                progress_bar=progress_bar,
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    def get_run_url(self, run_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/e/{sys_id}"

    def get_project_url(self, project_id: str, workspace: str, project_name: str) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/"

    def get_model_url(self, model_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/m/{sys_id}"

    def get_model_version_url(
        self,
        model_version_id: str,
        model_id: str,
        workspace: str,
        project_name: str,
        sys_id: str,
    ) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/m/{model_id}/v/{sys_id}"


def _get_column_type_from_entries(entries: List[Any], column: str) -> str:
    if not entries:  # column chosen is not present in the table
        raise ValueError(f"Column '{column}' chosen for sorting is not present in the table")

    if len(entries) == 1 and entries[0].name == column:
        return entries[0].type

    types = set()
    for entry in entries:
        if entry.name != column:  # caught by regex, but it's not this column
            continue
        if entry.type not in ATOMIC_ATTRIBUTE_TYPES:  # non-atomic type - no need to look further
            raise ValueError(
                f"Column {column} used for sorting is a complex type. For more, "
                f"see https://docs.neptune.ai/api/field_types/#simple-types"
            )
        types.add(entry.type)

    if types == {AttributeType.INT.value, AttributeType.FLOAT.value}:
        return AttributeType.FLOAT.value

    warn_once(
        f"Column {column} contains more than one simple data type. Sorting result might be inaccurate.",
        exception=NeptuneWarning,
    )
    return AttributeType.STRING.value
