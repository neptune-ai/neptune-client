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
    Union,
)

from neptune.common.deprecation import warn_once
from neptune.new.internal.utils import (
    is_bool,
    is_dict_like,
    is_float,
    is_float_like,
    is_int,
    is_string,
    is_string_like,
    is_stringify_value,
)
from neptune.new.types import (
    Boolean,
    File,
    Integer,
)
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.namespace import Namespace
from neptune.new.types.series import (
    FileSeries,
    FloatSeries,
    StringSeries,
)
from neptune.new.types.series.series import Series
from neptune.new.types.value import Value
from neptune.new.types.value_copy import ValueCopy


def cast_value(value: Any) -> Value:
    from neptune.new.handler import Handler

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
    elif is_string_like(value):
        if not from_stringify_value:
            warn_once(
                message="The object you're logging will be implicitly cast to a string."
                " We'll end support of this behavior in `neptune-client==1.0.0`."
                " To log the object as a string, use `str(object)` or `repr(object)` instead."
                " For details, see https://docs.neptune.ai/setup/neptune-client_1-0_release_changes"
            )
        return String(str(value))
    else:
        raise TypeError("Value of unsupported type {}".format(type(value)))


def cast_value_for_extend(values: Union[Namespace, Series, Collection[Any]]) -> Union[Series, Namespace]:
    if isinstance(values, Namespace):
        return values
    elif is_dict_like(values):
        return Namespace(values)
    elif isinstance(values, Series):
        return values

    sample_val = next(iter(values))

    from_stringify_value = False
    if is_stringify_value(sample_val):
        from_stringify_value, sample_val = True, sample_val.value

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
    elif is_string_like(sample_val):
        if not from_stringify_value:
            warn_once(
                message="The object you're logging will be implicitly cast to a string."
                " We'll end support of this behavior in `neptune-client==1.0.0`."
                " To log the object as a string, use `str(object)` or"
                " `stringify_unsupported(collection)` for collections and dictionaries."
                " For details, see https://docs.neptune.ai/setup/neptune-client_1-0_release_changes"
            )
        return StringSeries(values=values)
    else:
        raise TypeError("Value of unsupported type List[{}]".format(type(sample_val)))
