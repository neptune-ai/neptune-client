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

from neptune.api.models import (
    BoolField,
    DateTimeField,
    Field,
    FieldDefinition,
    FieldType,
    FloatField,
    FloatSeriesField,
    FloatSeriesValues,
    IntField,
    LeaderboardEntry,
    NextPage,
    QueryFieldDefinitionsResult,
    QueryFieldsResult,
    StringField,
    StringSeriesField,
    StringSeriesValues,
    StringSetField,
)
from neptune.api.proto.neptune_pb.api.model.attributes_pb2 import ProtoAttributesSearchResultDTO
from neptune.api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import ProtoAttributesDTO
from neptune.api.proto.neptune_pb.api.model.series_values_pb2 import ProtoFloatSeriesValuesDTO
from neptune.api.searching_entries import iter_over_pages
from neptune.core.components.operation_storage import OperationStorage
from neptune.envs import (
    NEPTUNE_FETCH_TABLE_STEP_SIZE,
    NEPTUNE_USE_PROTOCOL_BUFFERS,
)
from neptune.exceptions import (
    AmbiguousProjectName,
    ContainerUUIDNotFound,
    FetchAttributeNotFoundException,
    MetadataContainerNotFound,
    MetadataInconsistency,
    NeptuneFeatureNotAvailableException,
    NeptuneLimitExceedException,
    NeptuneObjectCreationConflict,
    ProjectNotFound,
    ProjectNotFoundWithSuggestions,
)
from neptune.internal.backends.api_model import (
    ApiExperiment,
    Project,
    Workspace,
)
from neptune.internal.backends.hosted_client import (
    DEFAULT_PROTO_REQUEST_KWARGS,
    DEFAULT_REQUEST_KWARGS,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
)
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.internal.backends.utils import (
    ExecuteOperationsBatchingManager,
    build_operation_url,
    ssl_verify,
    with_api_exceptions_handler,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.exceptions import NeptuneException
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.internal.operation import Operation
from neptune.internal.utils.generic_attribute_mapper import map_attribute_result_to_value
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.paths import path_to_str
from neptune.internal.utils.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.typing import ProgressBarType
from neptune.version import __version__

if TYPE_CHECKING:
    from bravado.requests_client import RequestsClient

    from neptune.internal.backends.api_model import ClientConfig


_logger = get_logger()

ATOMIC_ATTRIBUTE_TYPES = {
    FieldType.INT.value,
    FieldType.FLOAT.value,
    FieldType.STRING.value,
    FieldType.BOOL.value,
    FieldType.DATETIME.value,
    FieldType.OBJECT_STATE.value,
}


class HostedNeptuneBackend(NeptuneBackend):
    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials
        self.proxies = proxies
        self.missing_features = []
        self.use_proto = os.getenv(NEPTUNE_USE_PROTOCOL_BUFFERS, "False").lower() in {"true", "1", "y"}

        http_client, client_config = create_http_client_with_auth(
            credentials=credentials, ssl_verify=ssl_verify(), proxies=proxies
        )
        self._http_client: "RequestsClient" = http_client
        self._client_config: "ClientConfig" = client_config

        self.backend_client = create_backend_client(self._client_config, self._http_client)
        self.leaderboard_client = create_leaderboard_client(self._client_config, self._http_client)

    def verify_feature_available(self, feature_name: str):
        if not self._client_config.has_feature(feature_name):
            raise NeptuneFeatureNotAvailableException(feature_name)

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
        experiment = (
            self.leaderboard_client.api.getExperiment(
                experimentId=container_id,
                **DEFAULT_REQUEST_KWARGS,
            )
            .response()
            .result
        )

        if expected_container_type is not None and ContainerType.from_api(experiment.type) != expected_container_type:
            raise MetadataContainerNotFound.of_container_type(
                container_type=expected_container_type, container_id=container_id
            )

        return ApiExperiment.from_experiment(experiment)

    @with_api_exceptions_handler
    def create_run(
        self,
        project_id: UniqueId,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiExperiment:
        additional_params = {
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
            "cliVersion": __version__,
            **additional_params,
        }

        kwargs = {
            "experimentCreationParams": params,
            "X-Neptune-CliVersion": __version__,
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

        errors.extend(
            self._execute_operations(
                container_id,
                container_type,
                operations=preprocessed_operations.other_operations,
            )
        )

        for op in itertools.chain(
            preprocessed_operations.upload_operations,
            preprocessed_operations.other_operations,
        ):
            op.clean(operation_storage=operation_storage)

        return (
            operations_preprocessor.processed_ops_count + dropped_count,
            errors,
        )

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
    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[FieldDefinition]:
        params = {
            "experimentId": container_id,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            experiment = self.leaderboard_client.api.getExperimentAttributes(**params).response().result

            attribute_type_names = [at.value for at in FieldType]
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

            return [
                FieldDefinition.from_model(field) for field in accepted_attributes if field.type in attribute_type_names
            ]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e

    @with_api_exceptions_handler
    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatAttribute(**params).response().result
            return FloatField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getIntAttribute(**params).response().result
            return IntField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getBoolAttribute(**params).response().result
            return BoolField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> StringField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringAttribute(**params).response().result
            return StringField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DateTimeField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getDatetimeAttribute(**params).response().result
            return DateTimeField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesAttribute(**params).response().result
            return FloatSeriesField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesAttribute(**params).response().result
            return StringSeriesField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetField:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSetAttribute(**params).response().result
            return StringSetField.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
    ) -> StringSeriesValues:
        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            "limit": limit,
            "skipToStep": from_step,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesValues(**params).response().result
            return StringSeriesValues.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
        use_proto: Optional[bool] = None,
    ) -> FloatSeriesValues:
        use_proto = use_proto if use_proto is not None else self.use_proto

        params = {
            "experimentId": container_id,
            "attribute": path_to_str(path),
            "limit": limit,
            "skipToStep": from_step,
        }
        try:
            if use_proto:
                result = (
                    self.leaderboard_client.api.getFloatSeriesValuesProto(
                        **params,
                        **DEFAULT_PROTO_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                data = ProtoFloatSeriesValuesDTO.FromString(result)
                return FloatSeriesValues.from_proto(data)
            else:
                result = (
                    self.leaderboard_client.api.getFloatSeriesValues(
                        **params,
                        **DEFAULT_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                return FloatSeriesValues.from_model(result)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def query_fields_within_project(
        self,
        project_id: QualifiedName,
        field_names_filter: Optional[List[str]] = None,
        experiment_ids_filter: Optional[List[str]] = None,
        next_page: Optional[NextPage] = None,
    ) -> QueryFieldsResult:
        pagination = {"nextPage": next_page.to_dto()} if next_page else {}
        params = {
            "projectIdentifier": project_id,
            "query": {
                **pagination,
                "attributeNamesFilter": field_names_filter,
                "experimentIdsFilter": experiment_ids_filter,
            },
            **DEFAULT_REQUEST_KWARGS,
        }

        try:
            result = self.leaderboard_client.api.queryAttributesWithinProject(**params).response().result
            return QueryFieldsResult.from_model(result)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_id)

    @with_api_exceptions_handler
    def fetch_atom_attribute_values(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> List[Tuple[str, FieldType, Any]]:
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
        step_size: Optional[int] = None,
        use_proto: Optional[bool] = None,
    ) -> Generator[LeaderboardEntry, None, None]:
        use_proto = use_proto if use_proto is not None else self.use_proto
        default_step_size = step_size or int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "100"))

        step_size = min(default_step_size, limit) if limit else default_step_size

        types_filter = list(map(lambda container_type: container_type.to_api(), types)) if types else None
        attributes_filter = {"attributeFilters": [{"path": column} for column in columns]} if columns else {}

        if sort_by == "sys/creation_time":
            sort_by_column_type = FieldType.DATETIME.value
        elif sort_by == "sys/id":
            sort_by_column_type = FieldType.STRING.value
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
                use_proto=use_proto,
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    def query_fields_definitions_within_project(
        self,
        project_id: QualifiedName,
        field_name_regex: Optional[str] = None,
        experiment_ids_filter: Optional[List[str]] = None,
        next_page: Optional[NextPage] = None,
    ) -> QueryFieldDefinitionsResult:
        pagination = {"nextPage": next_page.to_dto()} if next_page else {}
        params = {
            "projectIdentifier": project_id,
            "query": {
                **pagination,
                "experimentIdsFilter": experiment_ids_filter,
                "attributeNameRegex": field_name_regex,
            },
        }

        try:
            data = (
                self.leaderboard_client.api.queryAttributeDefinitionsWithinProject(
                    **params,
                    **DEFAULT_REQUEST_KWARGS,
                )
                .response()
                .result
            )
            return QueryFieldDefinitionsResult.from_model(data)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_id)

    def get_fields_definitions(
        self,
        container_id: str,
        container_type: ContainerType,
        use_proto: Optional[bool] = None,
    ) -> List[FieldDefinition]:
        use_proto = use_proto if use_proto is not None else self.use_proto

        params = {
            "experimentIdentifier": container_id,
        }

        try:
            if use_proto:
                result = (
                    self.leaderboard_client.api.queryAttributeDefinitionsProto(
                        **params,
                        **DEFAULT_PROTO_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                data = ProtoAttributesSearchResultDTO.FromString(result)
                return [FieldDefinition.from_proto(field_def) for field_def in data.entries]
            else:
                data = (
                    self.leaderboard_client.api.queryAttributeDefinitions(
                        **params,
                        **DEFAULT_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                return [FieldDefinition.from_model(field_def) for field_def in data.entries]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e

    def get_fields_with_paths_filter(
        self, container_id: str, container_type: ContainerType, paths: List[str], use_proto: Optional[bool] = None
    ) -> List[Field]:
        use_proto = use_proto if use_proto is not None else self.use_proto

        params = {
            "holderIdentifier": container_id,
            "holderType": "experiment",
            "attributeQuery": {
                "attributePathsFilter": paths,
            },
        }

        try:
            if use_proto:
                result = (
                    self.leaderboard_client.api.getAttributesWithPathsFilterProto(
                        **params,
                        **DEFAULT_PROTO_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                data = ProtoAttributesDTO.FromString(result)
                return [Field.from_proto(field) for field in data.attributes]
            else:
                data = (
                    self.leaderboard_client.api.getAttributesWithPathsFilter(
                        **params,
                        **DEFAULT_REQUEST_KWARGS,
                    )
                    .response()
                    .result
                )
                return [Field.from_model(field) for field in data.attributes]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e


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

    if types == {FieldType.INT.value, FieldType.FLOAT.value}:
        return FieldType.FLOAT.value

    warn_once(
        f"Column {column} contains more than one simple data type. Sorting result might be inaccurate.",
        exception=NeptuneWarning,
    )
    return FieldType.STRING.value
