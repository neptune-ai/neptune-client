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
from typing import Union

from neptune.new.internal.operation import AssignBool
from neptune.new.types.atoms.boolean import Boolean as BooleanVal
from neptune.new.attributes.atoms.atom import Atom


class Boolean(Atom):

    def assign(self, value: Union[BooleanVal, bool], wait: bool = False):
        if not isinstance(value, BooleanVal):
            value = BooleanVal(value)

        with self._run.lock():
            self._enqueue_operation(AssignBool(self._path, value.value), wait)

    def fetch(self) -> float:
        # pylint: disable=protected-access
        val = self._backend.get_bool_attribute(self._run_id, self._path)
        return val.value
