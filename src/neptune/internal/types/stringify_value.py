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


from typing import (
    Generic,
    TypeVar,
    Union,
)

T = TypeVar("T")


class StringifyValue(Generic[T]):
    def __init__(self, value: T) -> None:
        self.__value = value

    @property
    def value(self) -> T:
        return self.__value

    def __str__(self) -> str:
        return str(self.__value)

    def __repr__(self) -> str:
        return repr(self.__value)


def extract_if_stringify_value(val: Union[StringifyValue[T], T]) -> T:
    if isinstance(val, StringifyValue):
        return val.value
    return val
