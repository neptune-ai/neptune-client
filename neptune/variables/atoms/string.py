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
from neptune.internal.utils import verify_type

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import AssignString
from neptune.types.atoms.string import String as StringVal
from neptune.variables.atoms.atom import Atom

# pylint: disable=protected-access


class String(Atom):

    def assign(self, value: str, wait: bool = False):
        verify_type("value", value, str)
        with self._experiment.lock():
            self._experiment._op_processor.enqueue_operation(
                AssignString(self._experiment._uuid, self._path, value), wait)

    def get(self):
        val = self._experiment._backend.get(self._experiment._uuid, self._path)
        if not isinstance(val, StringVal):
            raise MetadataInconsistency("Variable {} is not a String".format(self._path))
        return val.value
