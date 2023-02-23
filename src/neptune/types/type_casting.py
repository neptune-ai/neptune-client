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
__all__ = ["cast_value", "cast_value_for_extend"]

import argparse
from datetime import datetime
from typing import (
    Any,
    Collection,
    Optional,
    Union,
)

from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import (
    is_bool,
    is_dict_like,
    is_float,
    is_float_like,
    is_int,
    is_string,
    is_stringify_value,
)
from neptune.types import (
    Boolean,
    File,
    Integer,
)
from neptune.types.atoms.datetime import Datetime
from neptune.types.atoms.float import Float
from neptune.types.atoms.string import String
from neptune.types.namespace import Namespace
from neptune.types.series import (
    FileSeries,
    FloatSeries,
    StringSeries,
)
from neptune.types.series.series import Series
from neptune.types.value import Value
from neptune.types.value_copy import ValueCopy


def cast_value(value: Any) -> Optional[Value]:
    from neptune.handler import Handler

    from_stringify_value = False
    if is_stringify_value(value):
        from_stringify_value, value = True, value.value

    if isinstance(value, Value):
        return value
    elif isinstance(value, Handler):
        return ValueCopy(value)
    elif isinstance(value, argparse.Namespace):
        return Namespace(vars(value))
    elif File.is_convertable_to_image(value):
        return File.as_image(value)
    elif File.is_convertable_to_html(value):
        return File.as_html(value)
    elif is_bool(value):
        return Boolean(value)
    elif is_int(value):
        return Integer(value)
    elif is_float(value):
        return Float(value)
    elif is_string(value):
        return String(value)
    elif isinstance(value, datetime):
        return Datetime(value)
    elif is_float_like(value):
        return Float(value)
    elif is_dict_like(value):
        return Namespace(value)
    elif from_stringify_value:
        return String(str(value))


def cast_value_for_extend(
    values: Union[StringifyValue, Namespace, Series, Collection[Any]]
) -> Optional[Union[Series, Namespace]]:
    from_stringify_value, original_values = False, None
    if is_stringify_value(values):
        from_stringify_value, original_values, values = True, values, values.value

    if isinstance(values, Namespace):
        return values
    elif is_dict_like(values):
        return Namespace(values)
    elif isinstance(values, Series):
        return values

    sample_val = next(iter(values))

    if isinstance(sample_val, File):
        return FileSeries(values=values)
    elif File.is_convertable_to_image(sample_val):
        return FileSeries(values=values)
    elif File.is_convertable_to_html(sample_val):
        return FileSeries(values=values)
    elif is_string(sample_val):
        return StringSeries(values=values)
    elif is_float_like(sample_val):
        return FloatSeries(values=values)
    elif from_stringify_value:
        return StringSeries(values=original_values)
