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
__all__ = ["NeptuneBackendMock"]

import os
import uuid
from collections import defaultdict
from datetime import datetime
from shutil import copyfile
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from zipfile import ZipFile

from neptune.api.dtos import FileEntry
from neptune.common.exceptions import (
    InternalClientError,
    NeptuneException,
)
from neptune.core.components.operation_storage import OperationStorage
from neptune.exceptions import (
    ContainerUUIDNotFound,
    MetadataInconsistency,
    ModelVersionNotFound,
    ProjectNotFound,
    RunNotFound,
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
    Project,
    StringAttribute,
    StringPointValue,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
    Workspace,
)
from neptune.internal.backends.hosted_file_operations import get_unique_upload_entries
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.container_structure import ContainerStructure
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    SysId,
    UniqueId,
)
from neptune.internal.operation import (
    AddStrings,
    AssignArtifact,
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    ClearArtifact,
    ClearFloatLog,
    ClearImageLog,
    ClearStringLog,
    ClearStringSet,
    ConfigFloatSeries,
    CopyAttribute,
    DeleteAttribute,
    DeleteFiles,
    LogFloats,
    LogImages,
    LogStrings,
    Operation,
    RemoveStrings,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.internal.operation_visitor import OperationVisitor
from neptune.internal.types.file_types import FileType
from neptune.internal.utils import base64_decode
from neptune.internal.utils.generic_attribute_mapper import NoValue
from neptune.internal.utils.git import GitInfo
from neptune.internal.utils.paths import path_to_str
from neptune.types import (
    Boolean,
    Integer,
)
from neptune.types.atoms import GitRef
from neptune.types.atoms.artifact import Artifact
from neptune.types.atoms.datetime import Datetime
from neptune.types.atoms.file import File
from neptune.types.atoms.float import Float
from neptune.types.atoms.string import String
from neptune.types.file_set import FileSet
from neptune.types.namespace import Namespace
from neptune.types.series.file_series import FileSeries
from neptune.types.series.float_series import FloatSeries
from neptune.types.series.string_series import StringSeries
from neptune.types.sets.string_set import StringSet
from neptune.types.value import Value
from neptune.types.value_visitor import ValueVisitor
from neptune.typing import ProgressBarType

Val = TypeVar("Val", bound=Value)


class NeptuneBackendMock(NeptuneBackend):
    WORKSPACE_NAME = "mock-workspace"
    PROJECT_NAME = "project-placeholder"
    PROJECT_KEY = SysId("OFFLINE")
    MODEL_SYS_ID = SysId("OFFLINE-MOD")

    def __init__(self, credentials=None, proxies=None):
        self._project_id: UniqueId = UniqueId(str(uuid.uuid4()))
        self._containers: Dict[(UniqueId, ContainerType), ContainerStructure[Value, dict]] = dict()
        self._next_run = 1  # counter for runs
        self._next_model_version = defaultdict(lambda: 1)  # counter for model versions
        self._artifacts: Dict[Tuple[str, str], List[ArtifactFileData]] = dict()
        self._attribute_type_converter_value_visitor = self.AttributeTypeConverterValueVisitor()
        self._create_container(self._project_id, ContainerType.PROJECT, self.PROJECT_KEY)

    def get_display_address(self) -> str:
        return "OFFLINE"

    def get_available_projects(
        self, workspace_id: Optional[str] = None, search_term: Optional[str] = None
    ) -> List[Project]:
        return [
            Project(
                id=UniqueId(str(uuid.uuid4())),
                name=self.PROJECT_NAME,
                workspace=self.WORKSPACE_NAME,
                sys_id=self.PROJECT_KEY,
            )
        ]

    def get_available_workspaces(self) -> List[Workspace]:
        return [Workspace(id=UniqueId(str(uuid.uuid4())), name=self.WORKSPACE_NAME)]

    def _create_container(self, container_id: UniqueId, container_type: ContainerType, sys_id: SysId):
        container = self._containers.setdefault((container_id, container_type), ContainerStructure[Value, dict]())
        container.set(["sys", "id"], String(str(sys_id)))
        container.set(["sys", "state"], String("Active"))
        container.set(["sys", "owner"], String("offline_user"))
        container.set(["sys", "size"], Float(0))
        container.set(["sys", "tags"], StringSet(set()))
        container.set(["sys", "creation_time"], Datetime(datetime.now()))
        container.set(["sys", "modification_time"], Datetime(datetime.now()))
        container.set(["sys", "failed"], Boolean(False))
        if container_type == ContainerType.MODEL_VERSION:
            container.set(["sys", "model_id"], String(str(self.MODEL_SYS_ID)))
            container.set(["sys", "stage"], String("none"))
        return container

    def _get_container(self, container_id: UniqueId, container_type: ContainerType):
        key = (container_id, container_type)
        if key not in self._containers:
            raise ContainerUUIDNotFound(container_id, container_type)
        container = self._containers[(container_id, container_type)]
        return container

    def create_run(
        self,
        project_id: UniqueId,
        git_info: Optional[GitInfo] = None,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiExperiment:
        sys_id = SysId(f"{self.PROJECT_KEY}-{self._next_run}")
        self._next_run += 1
        new_run_id = UniqueId(str(uuid.uuid4()))
        self._create_container(new_run_id, ContainerType.RUN, sys_id=sys_id)
        return ApiExperiment(
            id=new_run_id,
            type=ContainerType.RUN,
            sys_id=sys_id,
            workspace=self.WORKSPACE_NAME,
            project_name=self.PROJECT_NAME,
            trashed=False,
        )

    def create_model(self, project_id: str, key: str) -> ApiExperiment:
        sys_id = SysId(f"{self.PROJECT_KEY}-{key}")
        new_run_id = UniqueId(str(uuid.uuid4()))
        self._create_container(new_run_id, ContainerType.MODEL, sys_id=sys_id)
        return ApiExperiment(
            id=new_run_id,
            type=ContainerType.MODEL,
            sys_id=sys_id,
            workspace=self.WORKSPACE_NAME,
            project_name=self.PROJECT_NAME,
            trashed=False,
        )

    def create_model_version(self, project_id: str, model_id: UniqueId) -> ApiExperiment:
        try:
            model_key = self._get_container(container_id=model_id, container_type=ContainerType.MODEL).get("sys/id")
        except ContainerUUIDNotFound:
            model_key = "MOD"

        sys_id = SysId(f"{self.PROJECT_KEY}-{model_key}-{self._next_model_version[model_id]}")
        self._next_model_version[model_id] += 1
        new_run_id = UniqueId(str(uuid.uuid4()))
        self._create_container(new_run_id, ContainerType.MODEL_VERSION, sys_id=sys_id)
        return ApiExperiment(
            id=new_run_id,
            type=ContainerType.MODEL,
            sys_id=sys_id,
            workspace=self.WORKSPACE_NAME,
            project_name=self.PROJECT_NAME,
            trashed=False,
        )

    def create_checkpoint(self, notebook_id: str, jupyter_path: str) -> Optional[str]:
        return None

    def get_project(self, project_id: QualifiedName) -> Project:
        return Project(
            id=self._project_id,
            name=self.PROJECT_NAME,
            workspace=self.WORKSPACE_NAME,
            sys_id=self.PROJECT_KEY,
        )

    def get_metadata_container(
        self,
        container_id: Union[UniqueId, QualifiedName],
        expected_container_type: Optional[ContainerType],
    ) -> ApiExperiment:
        if "/" not in container_id:
            raise ValueError("Backend mock expect container_id as QualifiedName only")

        if expected_container_type == ContainerType.RUN:
            raise RunNotFound(container_id)
        elif expected_container_type == ContainerType.MODEL:
            return ApiExperiment(
                id=UniqueId(str(uuid.uuid4())),
                type=ContainerType.MODEL,
                sys_id=SysId(container_id.rsplit("/", 1)[-1]),
                workspace=self.WORKSPACE_NAME,
                project_name=self.PROJECT_NAME,
            )
        elif expected_container_type == ContainerType.MODEL_VERSION:
            raise ModelVersionNotFound(container_id)
        else:
            raise ProjectNotFound(container_id)

    def execute_operations(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        operations: List[Operation],
        operation_storage: OperationStorage,
    ) -> Tuple[int, List[NeptuneException]]:
        result = []
        for op in operations:
            try:
                self._execute_operation(container_id, container_type, op, operation_storage)
            except NeptuneException as e:
                result.append(e)
        return len(operations), result

    def _execute_operation(
        self, container_id: UniqueId, container_type: ContainerType, op: Operation, operation_storage: OperationStorage
    ) -> None:
        run = self._get_container(container_id, container_type)
        val = run.get(op.path)
        if val is not None and not isinstance(val, Value):
            if isinstance(val, dict):
                raise MetadataInconsistency("{} is a namespace, not an attribute".format(op.path))
            else:
                raise InternalClientError("{} is a {}".format(op.path, type(val)))
        visitor = NeptuneBackendMock.NewValueOpVisitor(self, op.path, val, operation_storage)
        new_val = visitor.visit(op)
        if new_val is not None:
            run.set(op.path, new_val)
        else:
            run.pop(op.path)

    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        run = self._get_container(container_id, container_type)
        return list(self._generate_attributes(None, run.get_structure()))

    def _generate_attributes(self, base_path: Optional[str], values: dict):
        for key, value_or_dict in values.items():
            new_path = base_path + "/" + key if base_path is not None else key
            if isinstance(value_or_dict, dict):
                yield from self._generate_attributes(new_path, value_or_dict)
            else:
                yield Attribute(
                    new_path,
                    value_or_dict.accept(self._attribute_type_converter_value_visitor),
                )

    def download_file(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        run = self._get_container(container_id, container_type)
        value: File = run.get(path)
        target_path = os.path.abspath(destination or (path[-1] + ("." + value.extension if value.extension else "")))
        if value.file_type is FileType.IN_MEMORY:
            with open(target_path, "wb") as target_file:
                target_file.write(value.content)
        elif value.file_type is FileType.LOCAL_FILE:
            if value.path != target_path:
                copyfile(value.path, target_path)
        else:
            raise ValueError(f"Unexpected FileType: {value.file_type}")

    def download_file_set(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        run = self._get_container(container_id, container_type)
        source_file_set_value: FileSet = run.get(path)

        if destination is None:
            target_file = path[-1] + ".zip"
        elif os.path.isdir(destination):
            target_file = os.path.join(destination, path[-1] + ".zip")
        else:
            target_file = destination

        upload_entries = get_unique_upload_entries(source_file_set_value.file_globs)

        with ZipFile(target_file, "w") as zipObj:
            for upload_entry in upload_entries:
                zipObj.write(upload_entry.source, upload_entry.target_path)

    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatAttribute:
        val = self._get_attribute(container_id, container_type, path, Float)
        return FloatAttribute(val.value)

    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntAttribute:
        val = self._get_attribute(container_id, container_type, path, Integer)
        return IntAttribute(val.value)

    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolAttribute:
        val = self._get_attribute(container_id, container_type, path, Boolean)
        return BoolAttribute(val.value)

    def get_file_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FileAttribute:
        val = self._get_attribute(container_id, container_type, path, File)
        return FileAttribute(
            name=os.path.basename(val.path) if val.file_type is FileType.LOCAL_FILE else "",
            ext=val.extension or "",
            size=0,
        )

    def get_string_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringAttribute:
        val = self._get_attribute(container_id, container_type, path, String)
        return StringAttribute(val.value)

    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DatetimeAttribute:
        val = self._get_attribute(container_id, container_type, path, Datetime)
        return DatetimeAttribute(val.value)

    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactAttribute:
        val = self._get_attribute(container_id, container_type, path, Artifact)
        return ArtifactAttribute(val.hash)

    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        return self._artifacts[(project_id, artifact_hash)]

    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesAttribute:
        val = self._get_attribute(container_id, container_type, path, FloatSeries)
        return FloatSeriesAttribute(val.values[-1] if val.values else None)

    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesAttribute:
        val = self._get_attribute(container_id, container_type, path, StringSeries)
        return StringSeriesAttribute(val.values[-1] if val.values else None)

    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetAttribute:
        val = self._get_attribute(container_id, container_type, path, StringSet)
        return StringSetAttribute(set(val.values))

    def _get_attribute(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        expected_type: Type[Val],
    ) -> Val:
        run = self._get_container(container_id, container_type)
        value: Optional[Value] = run.get(path)
        str_path = path_to_str(path)
        if value is None:
            raise MetadataInconsistency("Attribute {} not found".format(str_path))
        if isinstance(value, expected_type):
            return value
        raise MetadataInconsistency("Attribute {} is not {}".format(str_path, type.__name__))

    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> StringSeriesValues:
        val = self._get_attribute(container_id, container_type, path, StringSeries)
        return StringSeriesValues(
            len(val.values),
            [StringPointValue(timestampMillis=42342, step=idx, value=v) for idx, v in enumerate(val.values)],
        )

    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> FloatSeriesValues:
        val = self._get_attribute(container_id, container_type, path, FloatSeries)
        return FloatSeriesValues(
            len(val.values),
            [FloatPointValue(timestampMillis=42342, step=idx, value=v) for idx, v in enumerate(val.values)],
        )

    def get_image_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> ImageSeriesValues:
        return ImageSeriesValues(0)

    def download_file_series_by_index(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        index: int,
        destination: str,
        progress_bar: Optional[ProgressBarType],
    ):
        """Non relevant for backend"""

    def get_run_url(self, run_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        return f"offline/{run_id}"

    def get_project_url(self, project_id: str, workspace: str, project_name: str) -> str:
        return f"offline/{project_id}"

    def get_model_url(self, model_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        return f"offline/{model_id}"

    def get_model_version_url(
        self,
        model_version_id: str,
        model_id: str,
        workspace: str,
        project_name: str,
        sys_id: str,
    ) -> str:
        return f"offline/{model_version_id}"

    def _get_attribute_values(self, value_dict, path_prefix: List[str]):
        assert isinstance(value_dict, dict)
        for k, value in value_dict.items():
            if isinstance(value, dict):
                yield from self._get_attribute_values(value, path_prefix + [k])
            else:
                attr_type = value.accept(self._attribute_type_converter_value_visitor).value
                attr_path = "/".join(path_prefix + [k])
                if hasattr(value, "value"):
                    yield attr_path, attr_type, value.value
                else:
                    return attr_path, attr_type, NoValue

    def fetch_atom_attribute_values(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> List[Tuple[str, AttributeType, Any]]:
        run = self._get_container(container_id, container_type)
        values = self._get_attribute_values(run.get(path), path)
        namespace_prefix = path_to_str(path)
        if namespace_prefix:
            # don't want to catch "ns/attribute/other" while looking for "ns/attr"
            namespace_prefix += "/"
        return [
            (full_path, attr_type, attr_value)
            for (full_path, attr_type, attr_value) in values
            if full_path.startswith(namespace_prefix)
        ]

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
        """Non relevant for mock"""

    class AttributeTypeConverterValueVisitor(ValueVisitor[AttributeType]):
        def visit_float(self, _: Float) -> AttributeType:
            return AttributeType.FLOAT

        def visit_integer(self, _: Integer) -> AttributeType:
            return AttributeType.INT

        def visit_boolean(self, _: Boolean) -> AttributeType:
            return AttributeType.BOOL

        def visit_string(self, _: String) -> AttributeType:
            return AttributeType.STRING

        def visit_datetime(self, _: Datetime) -> AttributeType:
            return AttributeType.DATETIME

        def visit_file(self, _: File) -> AttributeType:
            return AttributeType.FILE

        def visit_file_set(self, _: FileSet) -> AttributeType:
            return AttributeType.FILE_SET

        def visit_float_series(self, _: FloatSeries) -> AttributeType:
            return AttributeType.FLOAT_SERIES

        def visit_string_series(self, _: StringSeries) -> AttributeType:
            return AttributeType.STRING_SERIES

        def visit_image_series(self, _: FileSeries) -> AttributeType:
            return AttributeType.IMAGE_SERIES

        def visit_string_set(self, _: StringSet) -> AttributeType:
            return AttributeType.STRING_SET

        def visit_git_ref(self, _: GitRef) -> AttributeType:
            return AttributeType.GIT_REF

        def visit_artifact(self, _: Artifact) -> AttributeType:
            return AttributeType.ARTIFACT

        def visit_namespace(self, _: Namespace) -> AttributeType:
            raise NotImplementedError

        def copy_value(self, source_type: Type[Attribute], source_path: List[str]) -> AttributeType:
            raise NotImplementedError

    class NewValueOpVisitor(OperationVisitor[Optional[Value]]):
        def __init__(
            self, backend, path: List[str], current_value: Optional[Value], operation_storage: OperationStorage
        ):
            self._backend = backend
            self._path = path
            self._current_value = current_value
            self._artifact_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            self._operation_storage = operation_storage

        def visit_assign_float(self, op: AssignFloat) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Float):
                raise self._create_type_error("assign", Float.__name__)
            return Float(op.value)

        def visit_assign_int(self, op: AssignInt) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Integer):
                raise self._create_type_error("assign", Integer.__name__)
            return Integer(op.value)

        def visit_assign_bool(self, op: AssignBool) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Boolean):
                raise self._create_type_error("assign", Boolean.__name__)
            return Boolean(op.value)

        def visit_assign_string(self, op: AssignString) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, String):
                raise self._create_type_error("assign", String.__name__)
            return String(op.value)

        def visit_assign_datetime(self, op: AssignDatetime) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Datetime):
                raise self._create_type_error("assign", Datetime.__name__)
            return Datetime(op.value)

        def visit_assign_artifact(self, op: AssignArtifact) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Artifact):
                raise self._create_type_error("assign", Artifact.__name__)
            return Artifact(op.hash)

        def visit_track_files_to_artifact(self, _: TrackFilesToArtifact) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Artifact):
                raise self._create_type_error("save", Artifact.__name__)
            return Artifact(self._artifact_hash)

        def visit_clear_artifact(self, _: ClearArtifact) -> Optional[Value]:
            if self._current_value is None:
                return Artifact()
            if not isinstance(self._current_value, Artifact):
                raise self._create_type_error("clear", Artifact.__name__)
            return Artifact()

        def visit_upload_file(self, op: UploadFile) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, File):
                raise self._create_type_error("save", File.__name__)
            return File.from_path(path=op.get_absolute_path(self._operation_storage), extension=op.ext)

        def visit_upload_file_content(self, op: UploadFileContent) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, File):
                raise self._create_type_error("upload_files", File.__name__)
            return File.from_content(content=base64_decode(op.file_content), extension=op.ext)

        def visit_upload_file_set(self, op: UploadFileSet) -> Optional[Value]:
            if self._current_value is None or op.reset:
                return FileSet(op.file_globs)
            if not isinstance(self._current_value, FileSet):
                raise self._create_type_error("save", FileSet.__name__)
            return FileSet(self._current_value.file_globs + op.file_globs)

        def visit_log_floats(self, op: LogFloats) -> Optional[Value]:
            raw_values = [x.value for x in op.values]
            if self._current_value is None:
                return FloatSeries(raw_values)
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("log", FloatSeries.__name__)
            return FloatSeries(
                self._current_value.values + raw_values,
                min=self._current_value.min,
                max=self._current_value.max,
                unit=self._current_value.unit,
            )

        def visit_log_strings(self, op: LogStrings) -> Optional[Value]:
            raw_values = [x.value for x in op.values]
            if self._current_value is None:
                return StringSeries(raw_values)
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("log", StringSeries.__name__)
            return StringSeries(self._current_value.values + raw_values)

        def visit_log_images(self, op: LogImages) -> Optional[Value]:
            raw_values = [File.from_content(base64_decode(x.value.data)) for x in op.values]
            if self._current_value is None:
                return FileSeries(raw_values)
            if not isinstance(self._current_value, FileSeries):
                raise self._create_type_error("log", FileSeries.__name__)
            return FileSeries(self._current_value.values + raw_values)

        def visit_clear_float_log(self, op: ClearFloatLog) -> Optional[Value]:
            if self._current_value is None:
                return FloatSeries([])
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("clear", FloatSeries.__name__)
            return FloatSeries(
                [],
                min=self._current_value.min,
                max=self._current_value.max,
                unit=self._current_value.unit,
            )

        def visit_clear_string_log(self, op: ClearStringLog) -> Optional[Value]:
            if self._current_value is None:
                return StringSeries([])
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("clear", StringSeries.__name__)
            return StringSeries([])

        def visit_clear_image_log(self, op: ClearImageLog) -> Optional[Value]:
            if self._current_value is None:
                return FileSeries([])
            if not isinstance(self._current_value, FileSeries):
                raise self._create_type_error("clear", FileSeries.__name__)
            return FileSeries([])

        def visit_config_float_series(self, op: ConfigFloatSeries) -> Optional[Value]:
            if self._current_value is None:
                return FloatSeries([], min=op.min, max=op.max, unit=op.unit)
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("log", FloatSeries.__name__)
            return FloatSeries(self._current_value.values, min=op.min, max=op.max, unit=op.unit)

        def visit_add_strings(self, op: AddStrings) -> Optional[Value]:
            if self._current_value is None:
                return StringSet(op.values)
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("add", StringSet.__name__)
            return StringSet(self._current_value.values.union(op.values))

        def visit_remove_strings(self, op: RemoveStrings) -> Optional[Value]:
            if self._current_value is None:
                return StringSet(set())
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("remove", StringSet.__name__)
            return StringSet(self._current_value.values.difference(op.values))

        def visit_clear_string_set(self, op: ClearStringSet) -> Optional[Value]:
            if self._current_value is None:
                return StringSet(set())
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("clear", StringSet.__name__)
            return StringSet(set())

        def visit_delete_files(self, op: DeleteFiles) -> Optional[Value]:
            if self._current_value is None:
                return FileSet([])
            if not isinstance(self._current_value, FileSet):
                raise self._create_type_error("delete_files", FileSet.__name__)
            # It is not important to support deleting properly in debug mode, let's just ignore this operation
            return self._current_value

        def visit_delete_attribute(self, op: DeleteAttribute) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform delete operation on {}. Attribute is undefined.".format(self._path)
                )
            return None

        def visit_copy_attribute(self, op: CopyAttribute) -> Optional[Value]:
            return op.resolve(self._backend).accept(self)

        def _create_type_error(self, op_name, expected):
            return MetadataInconsistency(
                "Cannot perform {} operation on {}. Expected {}, {} found.".format(
                    op_name, self._path, expected, type(self._current_value)
                )
            )

    def list_fileset_files(self, attribute: List[str], container_id: str, path: str) -> List[FileEntry]:
        return [
            FileEntry(
                name="mock_name",
                size=100,
                mtime=datetime.now(),
                file_type="file",
            )
        ]
