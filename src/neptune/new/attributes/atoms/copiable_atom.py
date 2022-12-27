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
__all__ = ["CopiableAtom"]

import abc
import typing

from neptune.new.attributes.atoms.atom import Atom
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import CopyAttribute
from neptune.new.internal.utils.paths import parse_path
from neptune.new.types.value_copy import ValueCopy

if typing.TYPE_CHECKING:
    from neptune.new.internal.backends.neptune_backend import NeptuneBackend


class CopiableAtom(Atom):
    supports_copy = True

    def copy(self, value: ValueCopy, wait: bool = False):
        with self._container.lock():
            source_path = value.source_handler._path
            source_attr = value.source_handler._get_attribute()
            self._enqueue_operation(
                CopyAttribute(
                    self._path,
                    container_id=source_attr._container_id,
                    container_type=source_attr._container_type,
                    source_path=parse_path(source_path),
                    source_attr_cls=source_attr.__class__,
                ),
                wait,
            )

    @staticmethod
    @abc.abstractmethod
    def create_assignment_operation(path, value: int):
        ...

    @staticmethod
    @abc.abstractmethod
    def getter(
        backend: "NeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
    ) -> int:
        ...

    def fetch(self):
        return self.getter(self._backend, self._container_id, self._container_type, self._path)
