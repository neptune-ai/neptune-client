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
from neptune.new.internal.backends.api_model import AttributeType


class Omit:
    pass


_attribute_types_map = {
    AttributeType.FLOAT.value: "floatProperties",
    AttributeType.INT.value: "intProperties",
    AttributeType.BOOL.value: "boolProperties",
    AttributeType.STRING.value: "stringProperties",
    AttributeType.DATETIME.value: "datetimeProperties",
    AttributeType.FILE.value: None,
    AttributeType.FILE_SET.value: None,
    AttributeType.FLOAT_SERIES.value: None,
    AttributeType.STRING_SERIES.value: None,
    AttributeType.IMAGE_SERIES.value: None,
    AttributeType.STRING_SET.value: None,
    AttributeType.GIT_REF.value: "gitRefProperties",
    AttributeType.RUN_STATE.value: "experimentStateProperties",
    AttributeType.NOTEBOOK_REF.value: "notebookRefProperties",
}


def map_attribute_result_to_value(attribute):
    source_property = _attribute_types_map.get(attribute.type, None)
    if source_property is None:
        return Omit
    return getattr(attribute, source_property).value
