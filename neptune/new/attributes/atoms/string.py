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
import typing

import click

from neptune.new.attributes.atoms.copiable_atom import CopiableAtom
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import AssignString
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.atoms.string import String as StringVal

if typing.TYPE_CHECKING:
    from neptune.new.attribute_container import AttributeContainer
    from neptune.new.internal.backends.neptune_backend import NeptuneBackend


class String(CopiableAtom):

    MAX_VALUE_LENGTH = 16384

    def __init__(self, container: "AttributeContainer", path: typing.List[str]):
        super().__init__(container, path)
        self._value_truncation_occurred = False

    @staticmethod
    def create_assignment_operation(path, value: str):
        return AssignString(path, value)

    @staticmethod
    def getter(
        backend: "NeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
    ) -> str:
        val = backend.get_string_attribute(container_id, container_type, path)
        return val.value

    def assign(self, value: typing.Union[StringVal, str], wait: bool = False):
        if not isinstance(value, StringVal):
            value = StringVal(value)

        if len(value.value) > String.MAX_VALUE_LENGTH:
            value.value = value.value[: String.MAX_VALUE_LENGTH]

            if not self._value_truncation_occurred:
                # the first truncation
                self._value_truncation_occurred = True
                click.echo(
                    f"Warning: string '{path_to_str(self._path)}' value was "
                    f"longer than {String.MAX_VALUE_LENGTH} characters and was truncated. "
                    f"This warning is printed only once.",
                    err=True,
                )

        with self._container.lock():
            self._enqueue_operation(
                self.create_assignment_operation(self._path, value.value), wait
            )
