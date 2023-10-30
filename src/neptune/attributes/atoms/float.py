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
__all__ = ["Float"]

import typing

from neptune.attributes.atoms.copiable_atom import CopiableAtom
from neptune.common.warnings import (
    NeptuneUnsupportedValue,
    warn_once,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import AssignFloat
from neptune.internal.types.utils import is_unsupported_float
from neptune.types.atoms.float import Float as FloatVal

if typing.TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


class Float(CopiableAtom):
    @staticmethod
    def create_assignment_operation(path, value: float):
        return AssignFloat(path, value)

    @staticmethod
    def getter(
        backend: "NeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
    ) -> float:
        val = backend.get_float_attribute(container_id, container_type, path)
        return val.value

    def assign(self, value: typing.Union[FloatVal, float, int], *, wait: bool = False):
        if not isinstance(value, FloatVal):
            value = FloatVal(value)

        if is_unsupported_float(value.value):
            warn_once(
                message=f"WARNING: The value you're trying to log is a nonstandard float value ({str(value.value)}) "
                f"that is not currently supported. "
                f"We'll add support for this type of value in the future. "
                f"For now, you can use utils.stringify_unsupported() to log one or more values as strings: "
                f"run['field'] = stringify_unsupported(float({str(value.value)}))",
                exception=NeptuneUnsupportedValue,
            )
            return

        with self._container.lock():
            self._enqueue_operation(self.create_assignment_operation(self._path, value.value), wait=wait)
