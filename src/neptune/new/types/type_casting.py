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
import argparse
from datetime import datetime
from typing import Any

from neptune.common.deprecation import warn_once
from neptune.new.handler import Handler
from neptune.new.internal.utils import (
    is_bool,
    is_dict_like,
    is_float,
    is_float_like,
    is_int,
    is_string,
    is_string_like,
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
from neptune.new.types.value import Value
from neptune.new.types.value_copy import ValueCopy


def cast_value(value: Any) -> Value:
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
        return Float(float(value))
    elif is_dict_like(value):
        return Namespace(value)
    elif is_string_like(value):
        warn_once(
            message="The object you're logging will be implicitly cast to a string."
            " We'll end support of this behavior in `neptune-client==1.0.0`."
            " To log the object as a string, use `str(object)` instead.",
            stack_level=3,
        )
        return String(str(value))
    else:
        raise TypeError("Value of unsupported type {}".format(type(value)))
