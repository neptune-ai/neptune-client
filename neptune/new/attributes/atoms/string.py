#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from typing import List, TYPE_CHECKING, Union

import click

from neptune.new.internal.operation import AssignString
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.atoms.string import String as StringVal
from neptune.new.attributes.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.attribute_container import AttributeContainer


class String(Atom):

    MAX_VALUE_LENGTH = 16384

    def __init__(self, container: "AttributeContainer", path: List[str]):
        super().__init__(container, path)
        self._value_truncation_occurred = False

    def assign(self, value: Union[StringVal, str], wait: bool = False):
        if not isinstance(value, StringVal):
            value = StringVal(value)

        if (
            not self._value_truncation_occurred
            and len(value.value) > String.MAX_VALUE_LENGTH
        ):
            # the first truncation
            self._value_truncation_occurred = True
            click.echo(
                f"Warning: string '{path_to_str(self._path)}' value was "
                f"longer than {String.MAX_VALUE_LENGTH} characters and was truncated. "
                f"This warning is printed only once.",
                err=True,
            )
        value.value = value.value[: String.MAX_VALUE_LENGTH]

        with self._container.lock():
            self._enqueue_operation(AssignString(self._path, value.value), wait)

    def fetch(self) -> str:
        # pylint: disable=protected-access
        val = self._backend.get_string_attribute(self._container_id, self._path)
        return val.value
