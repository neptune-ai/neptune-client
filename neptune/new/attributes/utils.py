#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
from typing import List, TYPE_CHECKING

from neptune.new.attributes import (
    Boolean, Datetime, File, FileSeries, FileSet, Float, FloatSeries, GitRef, Integer, NotebookRef, RunState, String,
    StringSeries, StringSet,
)
from neptune.new.exceptions import InternalClientError
from neptune.new.internal.backends.api_model import AttributeType

if TYPE_CHECKING:
    from neptune.new import Run
    from neptune.new.attributes.attribute import Attribute

_attribute_type_to_attr_class_map = {
    AttributeType.FLOAT: Float,
    AttributeType.INT: Integer,
    AttributeType.BOOL: Boolean,
    AttributeType.STRING: String,
    AttributeType.DATETIME: Datetime,
    AttributeType.FILE: File,
    AttributeType.FILE_SET: FileSet,
    AttributeType.FLOAT_SERIES: FloatSeries,
    AttributeType.STRING_SERIES: StringSeries,
    AttributeType.IMAGE_SERIES: FileSeries,
    AttributeType.STRING_SET: StringSet,
    AttributeType.GIT_REF: GitRef,
    AttributeType.RUN_STATE: RunState,
    AttributeType.NOTEBOOK_REF: NotebookRef,
}


def create_attribute_from_type(attribute_type: AttributeType, run: 'Run', path: List[str]) -> 'Attribute':
    try:
        return _attribute_type_to_attr_class_map[attribute_type](run, path)
    except KeyError:
        raise InternalClientError(f"Unexpected type: {attribute_type}")
