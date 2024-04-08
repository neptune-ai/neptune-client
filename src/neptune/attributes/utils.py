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
__all__ = ["create_attribute_from_type"]

from typing import (
    TYPE_CHECKING,
    List,
)

from neptune.api.models import FieldType
from neptune.attributes import (
    Artifact,
    Boolean,
    Datetime,
    File,
    FileSeries,
    FileSet,
    Float,
    FloatSeries,
    GitRef,
    Integer,
    NotebookRef,
    RunState,
    String,
    StringSeries,
    StringSet,
)
from neptune.internal.exceptions import InternalClientError

if TYPE_CHECKING:
    from neptune.attributes.attribute import Attribute
    from neptune.objects import NeptuneObject

_attribute_type_to_attr_class_map = {
    FieldType.FLOAT: Float,
    FieldType.INT: Integer,
    FieldType.BOOL: Boolean,
    FieldType.STRING: String,
    FieldType.DATETIME: Datetime,
    FieldType.FILE: File,
    FieldType.FILE_SET: FileSet,
    FieldType.FLOAT_SERIES: FloatSeries,
    FieldType.STRING_SERIES: StringSeries,
    FieldType.IMAGE_SERIES: FileSeries,
    FieldType.STRING_SET: StringSet,
    FieldType.GIT_REF: GitRef,
    FieldType.OBJECT_STATE: RunState,
    FieldType.NOTEBOOK_REF: NotebookRef,
    FieldType.ARTIFACT: Artifact,
}


def create_attribute_from_type(
    attribute_type: FieldType,
    container: "NeptuneObject",
    path: List[str],
) -> "Attribute":
    try:
        return _attribute_type_to_attr_class_map[attribute_type](container, path)
    except KeyError:
        raise InternalClientError(f"Unexpected type: {attribute_type}")


def delayed_():
    pass
