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

from typing import Iterable

from neptune.internal.utils import verify_type

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import AddStrings, RemoveStrings, ClearStringSet
from neptune.types.sets.string_set import StringSet as StringSetVal
from neptune.variables.sets.set import Set


class StringSet(Set):

    def assign(self, value: StringSetVal, wait: bool = False):
        verify_type("value", value, StringSetVal)
        with self._experiment.lock():
            self.clear()
            self.add(value.values, wait)

    def add(self, values: Iterable[str], wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(AddStrings(self._experiment_uuid, self._path, list(values)), wait)

    def remove(self, values: Iterable[str], wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(RemoveStrings(self._experiment_uuid, self._path, list(values)), wait)

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(ClearStringSet(self._experiment_uuid, self._path), wait)

    def get(self):
        # pylint: disable=protected-access
        val = self._backend.get_attribute(self._experiment_uuid, self._path)
        if  not isinstance(val, StringSetVal):
            raise MetadataInconsistency("Variable {} is not a StringSet".format(self._path))
        return val.values
