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

from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import AssignInt, CopyAttribute
from neptune.new.internal.utils.paths import parse_path
from neptune.new.types.atoms.integer import Integer as IntegerVal
from neptune.new.attributes.atoms.atom import Atom
from neptune.new.types.value_copy import ValueCopy

if typing.TYPE_CHECKING:
    from neptune.new.internal.backends.neptune_backend import NeptuneBackend


class Integer(Atom):
    def copy(self, value: ValueCopy, wait: bool = False):
        # pylint: disable=protected-access
        with self._container.lock():
            source_path = value.source_handler._path
            source_attr = value.source_handler._run.get_attribute(source_path)
            self._enqueue_operation(
                CopyAttribute(
                    self._path,
                    value.source_handler._container_id,
                    value.source_handler._container_type,
                    parse_path(source_path),
                    source_attr.__class__,
                ),
                wait,
            )

    def assign(self, value: typing.Union[IntegerVal, float, int], wait: bool = False):
        if not isinstance(value, IntegerVal):
            value = IntegerVal(value)

        with self._container.lock():
            self._enqueue_operation(
                self.create_assignment_operation(self._path, value.value), wait
            )

    @staticmethod
    def create_assignment_operation(path, value: int):
        return AssignInt(path, value)

    @staticmethod
    def getter(
        backend: "NeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> int:
        val = backend.get_int_attribute(container_id, container_type, path)
        return val.value

    def fetch(self) -> int:
        return self.getter(self._backend, self._container_id, self._container_type, self._path)
