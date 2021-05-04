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
from neptune.new.attributes import (
    Boolean, Datetime, File, FileSeries, FileSet, Float, FloatSeries, GitRef, Integer, NotebookRef, RunState, String,
    StringSeries, StringSet,
)
from neptune.new.internal.backends.api_model import AttributeType

attribute_type_to_atom = {
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
