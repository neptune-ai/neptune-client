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

#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["StringifyValue", "extract_if_stringify_value"]

import math
from typing import Any

from neptune.constants import (
    MAX_32_BIT_INT,
    MIN_32_BIT_INT,
)
from neptune.internal.utils.logger import get_logger

logger = get_logger()


def is_unsupported_float(value) -> bool:
    if isinstance(value, float):
        return math.isinf(value) or math.isnan(value)
    return False


class StringifyValue:
    def __init__(self, value: Any):
        # check if it's an integer outside 32bit range and cast it to float
        if isinstance(value, int) and (value > MAX_32_BIT_INT or value < MIN_32_BIT_INT):
            logger.info(
                "Value '%d' is outside the range of 32-bit integers ('%d' to '%d') and will be logged as float",
                value,
                MIN_32_BIT_INT,
                MAX_32_BIT_INT,
            )
            value = float(value)
        if is_unsupported_float(value):
            value = str(value)

        self.__value = value

    @property
    def value(self):
        return self.__value

    def __str__(self):
        return str(self.__value)

    def __repr__(self):
        return repr(self.__value)


def extract_if_stringify_value(val):
    if isinstance(val, StringifyValue):
        return val.value
    return val
