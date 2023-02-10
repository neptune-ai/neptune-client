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
import logging
import os
import re
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
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

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.exceptions import (
    ClientHttpError,
    InternalClientError,
    NeptuneException,
)
from neptune.common.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.management.exceptions import ObjectNotFound
from neptune.new.envs import NEPTUNE_FETCH_TABLE_STEP_SIZE
from neptune.new.exceptions import (
    AmbiguousProjectName,
    ArtifactNotFoundException,
    ContainerUUIDNotFound,
    FetchAttributeNotFoundException,
    MetadataContainerNotFound,
    MetadataInconsistency,
    NeptuneFeatureNotAvailableException,
    NeptuneLegacyProjectException,
    NeptuneLimitExceedException,
    NeptuneObjectCreationConflict,
    ProjectNotFound,
    ProjectNotFoundWithSuggestions,
)
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.backends.api_model import (
    ApiExperiment,
    ArtifactAttribute,
    Attribute,
    AttributeType,
    AttributeWithProperties,
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
from neptune.new.internal.backends.hosted_artifact_operations import (
    track_to_existing_artifact,
    track_to_new_artifact,
)
from neptune.new.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    create_artifacts_client,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
)
from neptune.new.internal.backends.hosted_file_operations import (
    download_file_attribute,
    download_file_set_attribute,
    download_image_series_element,
    upload_file_attribute,
    upload_file_set_attribute,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.nql import NQLQuery
from neptune.new.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.new.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.new.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.new.internal.backends.utils import (
    ExecuteOperationsBatchingManager,
    MissingApiClient,
    build_operation_url,
    ssl_verify,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.new.internal.operation import (
    DeleteAttribute,
    Operation,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.new.internal.operation_processors.operation_storage import OperationStorage
from neptune.new.internal.utils import base64_decode
from neptune.new.internal.utils.generic_attribute_mapper import map_attribute_result_to_value
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef
from neptune.new.version import version as neptune_client_version

if TYPE_CHECKING:
    from bravado.requests_client import RequestsClient

    from neptune.new.internal.backends.api_model import ClientConfig


_logger = logging.getLogger(__name__)


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
        git_ref: Optional[GitRef] = None,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiExperiment:

        git_info = (
            {
                "commit": {
                    "commitId": git_ref.commit_id,
                    "message": git_ref.message,
                    "authorName": git_ref.author_name,
                    "authorEmail": git_ref.author_email,
                    "commitDate": git_ref.commit_date,
                },
                "repositoryDirty": git_ref.dirty,
                "currentBranch": git_ref.branch,
                "remotes": git_ref.remotes,
            }
            if git_ref
            else None
        )

        additional_params = {
            "gitInfo": git_info,
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
                operations=itertools.chain(assign_artifact_operations, preprocessed_operations.other_operations),
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
        operations: Iterable[Operation],
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
                    "Ignored following attributes (unknown type): %s.\n" "Try to upgrade `neptune-client.",
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
    ):
        try:
            download_image_series_element(
                swagger_client=self.leaderboard_client,
                container_id=container_id,
                attribute=path_to_str(path),
                index=index,
                destination=destination,
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
    ):
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                container_id=container_id,
                attribute=path_to_str(path),
                destination=destination,
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
    ):
        download_request = self._get_file_set_download_request(container_id, container_type, path)
        try:
            download_file_set_attribute(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination,
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

    @with_api_exceptions_handler
    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactAttribute:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getArtifactAttribute(**params).response().result
            return ArtifactAttribute(hash=result.hash)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        params = {
            "projectIdentifier": project_id,
            "hash": artifact_hash,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.artifacts_client.api.listArtifactFiles(**params).response().result
            return [ArtifactFileData.from_dto(a) for a in result.files]
        except HTTPNotFound:
            raise ArtifactNotFoundException(artifact_hash)

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
    def search_leaderboard_entries(
        self,
        project_id: UniqueId,
        types: Optional[Iterable[ContainerType]] = None,
        query: Optional[NQLQuery] = None,
        columns: Optional[Iterable[str]] = None,
    ) -> List[LeaderboardEntry]:
        if query:
            query_params = {"query": {"query": str(query)}}
        else:
            query_params = {}
        if columns:
            attributes_filter = {"attributeFilters": [{"path": column} for column in columns]}
        else:
            attributes_filter = {}

        def get_portion(limit, offset):
            return (
                self.leaderboard_client.api.searchLeaderboardEntries(
                    projectIdentifier=project_id,
                    type=list(map(lambda container_type: container_type.to_api(), types)),
                    params={
                        **query_params,
                        **attributes_filter,
                        "pagination": {"limit": limit, "offset": offset},
                    },
                    **DEFAULT_REQUEST_KWARGS,
                )
                .response()
                .result.entries
            )

        def to_leaderboard_entry(entry) -> LeaderboardEntry:
            supported_attribute_types = {item.value for item in AttributeType}
            attributes: List[AttributeWithProperties] = []
            for attr in entry.attributes:
                if attr.type in supported_attribute_types:
                    properties = attr.__getitem__("{}Properties".format(attr.type))
                    attributes.append(AttributeWithProperties(attr.name, AttributeType(attr.type), properties))
            return LeaderboardEntry(entry.experimentId, attributes)

        try:
            step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "100"))
            return [to_leaderboard_entry(e) for e in self._get_all_items(get_portion, step=step_size)]
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

    @staticmethod
    def _get_all_items(get_portion, step):
        max_server_offset = 10000
        items = []
        previous_items = None
        while (previous_items is None or len(previous_items) >= step) and len(items) < max_server_offset:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items
        return items
