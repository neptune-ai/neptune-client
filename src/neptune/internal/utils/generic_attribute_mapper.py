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
__all__ = ["NoValue", "atomic_attribute_types_map", "map_attribute_result_to_value"]

from neptune.api.models import FieldType


class NoValue:
    pass


VALUE = "value"
LAST_VALUE = "last"
VALUES = "values"

atomic_attribute_types_map = {
    FieldType.FLOAT.value: "floatProperties",
    FieldType.INT.value: "intProperties",
    FieldType.BOOL.value: "boolProperties",
    FieldType.STRING.value: "stringProperties",
    FieldType.DATETIME.value: "datetimeProperties",
    FieldType.OBJECT_STATE.value: "experimentStateProperties",
    FieldType.NOTEBOOK_REF.value: "notebookRefProperties",
}

value_series_attribute_types_map = {
    FieldType.FLOAT_SERIES.value: "floatSeriesProperties",
    FieldType.STRING_SERIES.value: "stringSeriesProperties",
}

value_set_attribute_types_map = {
    FieldType.STRING_SET.value: "stringSetProperties",
}

# TODO: nicer mapping?
_unmapped_attribute_types_map = {
    FieldType.FILE_SET.value: "fileSetProperties",  # TODO: return size?
    FieldType.FILE.value: "fileProperties",  # TODO: name? size?
    FieldType.IMAGE_SERIES.value: "imageSeriesProperties",  # TODO: return last step?
    FieldType.GIT_REF.value: "gitRefProperties",  # TODO: commit? branch?
}


def map_attribute_result_to_value(attribute):
    for attribute_map, value_key in [
        (atomic_attribute_types_map, VALUE),
        (value_series_attribute_types_map, LAST_VALUE),
        (value_set_attribute_types_map, VALUES),
    ]:
        source_property = attribute_map.get(attribute.type)
        if source_property is not None:
            mapped_attribute_entry = getattr(attribute, source_property)
            return getattr(mapped_attribute_entry, value_key)
    return NoValue
