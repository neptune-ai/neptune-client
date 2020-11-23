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
import uuid
from datetime import datetime
from shutil import copyfile
from typing import Optional, List, Dict

from neptune.alpha.exceptions import MetadataInconsistency, InternalClientError, ExperimentUUIDNotFound, \
    ExperimentNotFound, NeptuneException
from neptune.alpha.internal.backends.api_model import Project, Experiment, Attribute, AttributeType
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.experiment_structure import ExperimentStructure
from neptune.alpha.internal.operation import Operation, DeleteAttribute, \
    AssignString, AssignFloat, \
    LogStrings, LogFloats, LogImages, \
    ClearFloatLog, ClearStringLog, ClearStringSet, ClearImageLog, \
    RemoveStrings, AddStrings, \
    UploadFile, AssignDatetime, ConfigFloatSeries, UploadFileSet
from neptune.alpha.internal.operation_visitor import OperationVisitor
from neptune.alpha.types.atoms import GitRef
from neptune.alpha.types.atoms.datetime import Datetime
from neptune.alpha.types.atoms.file import File
from neptune.alpha.types.atoms.float import Float
from neptune.alpha.types.atoms.string import String
from neptune.alpha.types.file_set import FileSet
from neptune.alpha.types.series.float_series import FloatSeries
from neptune.alpha.types.series.image_series import ImageSeries
from neptune.alpha.types.series.string_series import StringSeries
from neptune.alpha.types.sets.string_set import StringSet
from neptune.alpha.types.value import Value
from neptune.alpha.types.value_visitor import ValueVisitor


class NeptuneBackendMock(NeptuneBackend):

    def __init__(self, credentials=None):
        # pylint: disable=unused-argument
        self._experiments: Dict[uuid.UUID, ExperimentStructure[Value]] = dict()
        self._attribute_type_converter_value_visitor = self.AttributeTypeConverterValueVisitor()

    def get_display_address(self) -> str:
        return "OFFLINE"

    def get_project(self, project_id: str) -> Project:
        return Project(uuid.uuid4(), "sandbox", "workspace")

    def create_experiment(self, project_uuid: uuid.UUID, git_ref: Optional[GitRef] = None) -> Experiment:
        short_id = "OFFLINE-{}".format(len(self._experiments) + 1)
        new_experiment_uuid = uuid.uuid4()
        self._experiments[new_experiment_uuid] = ExperimentStructure[Value]()
        self._experiments[new_experiment_uuid].set(["sys", "id"], String(short_id))
        self._experiments[new_experiment_uuid].set(["sys", "creation_time"], Datetime(datetime.now()))
        self._experiments[new_experiment_uuid].set(["sys", "modification_time"], Datetime(datetime.now()))
        if git_ref:
            self._experiments[new_experiment_uuid].set(["sys", "git"], git_ref)
        return Experiment(new_experiment_uuid, short_id, 'workspace', 'sandbox')

    def get_experiment(self, experiment_id: str) -> Experiment:
        raise ExperimentNotFound(experiment_id)

    def execute_operations(self, experiment_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        result = []
        for op in operations:
            try:
                self._execute_operation(experiment_uuid, op)
            except NeptuneException as e:
                result.append(e)
        return result

    def _execute_operation(self, experiment_uuid: uuid.UUID, op: Operation) -> None:
        if experiment_uuid not in self._experiments:
            raise ExperimentUUIDNotFound(experiment_uuid)
        exp = self._experiments[experiment_uuid]
        val = exp.get(op.path)
        if val is not None and not isinstance(val, Value):
            if isinstance(val, dict):
                raise MetadataInconsistency("{} is a namespace, not an attribute".format(op.path))
            else:
                raise InternalClientError("{} is a {}".format(op.path, type(val)))
        visitor = NeptuneBackendMock.NewValueOpVisitor(op.path, val)
        new_val = visitor.visit(op)
        if new_val:
            exp.set(op.path, new_val)
        else:
            exp.pop(op.path)

    def get_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> Value:
        return self._experiments[experiment_uuid].get(path)

    def get_attributes(self, experiment_uuid: uuid.UUID) -> List[Attribute]:
        if experiment_uuid not in self._experiments:
            raise ExperimentUUIDNotFound(experiment_uuid)
        exp = self._experiments[experiment_uuid]
        return list(self._generate_attributes(None, exp.get_structure()))

    def _generate_attributes(self, base_path: Optional[str], values: dict):
        for key, value_or_dict in values.items():
            new_path = base_path + '/' + key if base_path is not None else key
            if isinstance(value_or_dict, dict):
                yield from self._generate_attributes(new_path, value_or_dict)
            else:
                yield Attribute(new_path, value_or_dict.accept(self._attribute_type_converter_value_visitor))

    def download_file(self, experiment_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        source_file_value: File = self._experiments[experiment_uuid].get(path)
        source_path = source_file_value.file_path
        target_path = os.path.abspath(destination or os.path.basename(source_path))
        if source_path != target_path:
            copyfile(source_path, target_path)

    class AttributeTypeConverterValueVisitor(ValueVisitor[AttributeType]):

        def visit_float(self, _: Float) -> AttributeType:
            return AttributeType.FLOAT

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

        def visit_image_series(self, _: ImageSeries) -> AttributeType:
            return AttributeType.IMAGE_SERIES

        def visit_string_set(self, _: StringSet) -> AttributeType:
            return AttributeType.STRING_SET

        def visit_git_ref(self, _: GitRef) -> AttributeType:
            return AttributeType.GIT_REF

    class NewValueOpVisitor(OperationVisitor[Optional[Value]]):

        def __init__(self, path: List[str], current_value: Optional[Value]):
            self._path = path
            self._current_value = current_value

        def visit_assign_float(self, op: AssignFloat) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Float):
                raise self._create_type_error("assign", Float.__name__)
            return Float(op.value)

        def visit_assign_string(self, op: AssignString) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, String):
                raise self._create_type_error("assign", String.__name__)
            return String(op.value)

        def visit_assign_datetime(self, op: AssignDatetime) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Datetime):
                raise self._create_type_error("assign", Datetime.__name__)
            return Datetime(op.value)

        def visit_upload_file(self, op: UploadFile) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, File):
                raise self._create_type_error("save", File.__name__)
            return File(op.file_path)

        def visit_upload_file_set(self, op: UploadFileSet) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, FileSet):
                raise self._create_type_error("save", FileSet.__name__)
            return FileSet(op.file_globs)

        def visit_log_floats(self, op: LogFloats) -> Optional[Value]:
            raw_values = [x.value for x in op.values]
            if self._current_value is None:
                return FloatSeries(raw_values)
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("log", FloatSeries.__name__)
            return FloatSeries(self._current_value.values + raw_values,
                               min=self._current_value.min,
                               max=self._current_value.max,
                               unit=self._current_value.unit)

        def visit_log_strings(self, op: LogStrings) -> Optional[Value]:
            raw_values = [x.value for x in op.values]
            if self._current_value is None:
                return StringSeries(raw_values)
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("log", StringSeries.__name__)
            return StringSeries(self._current_value.values + raw_values)

        def visit_log_images(self, op: LogImages) -> Optional[Value]:
            raw_values = [x.value for x in op.values]
            if self._current_value is None:
                return ImageSeries(raw_values)
            if not isinstance(self._current_value, ImageSeries):
                raise self._create_type_error("log", ImageSeries.__name__)
            return ImageSeries(self._current_value.values + raw_values)

        def visit_clear_float_log(self, op: ClearFloatLog) -> Optional[Value]:
            # pylint: disable=unused-argument
            if self._current_value is None:
                return FloatSeries([])
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("clear", FloatSeries.__name__)
            return FloatSeries([],
                               min=self._current_value.min,
                               max=self._current_value.max,
                               unit=self._current_value.unit)

        def visit_clear_string_log(self, op: ClearStringLog) -> Optional[Value]:
            # pylint: disable=unused-argument
            if self._current_value is None:
                return StringSeries([])
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("clear", StringSeries.__name__)
            return StringSeries([])

        def visit_clear_image_log(self, op: ClearImageLog) -> Optional[Value]:
            # pylint: disable=unused-argument
            if self._current_value is None:
                return ImageSeries([])
            if not isinstance(self._current_value, ImageSeries):
                raise self._create_type_error("clear", ImageSeries.__name__)
            return ImageSeries([])

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
            # pylint: disable=unused-argument
            if self._current_value is None:
                return StringSet(set())
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("clear", StringSet.__name__)
            return StringSet(set())

        def visit_delete_attribute(self, op: DeleteAttribute) -> Optional[Value]:
            # pylint: disable=unused-argument
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform delete operation on {}. Attribute is undefined.".format(self._path))
            return None

        def _create_type_error(self, op_name, expected):
            return MetadataInconsistency("Cannot perform {} operation on {}. Expected {}, {} found."
                                         .format(op_name, self._path, expected, type(self._current_value)))
