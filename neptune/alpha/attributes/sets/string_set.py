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

from typing import Iterable, Union
import typing

from neptune.alpha.internal.utils import verify_type, verify_collection_type, is_collection

from neptune.alpha.internal.operation import AddStrings, RemoveStrings, ClearStringSet
from neptune.alpha.types.sets.string_set import StringSet as StringSetVal
from neptune.alpha.attributes.sets.set import Set


class StringSet(Set):

    def assign(self, value: StringSetVal, wait: bool = False):
        verify_type("value", value, StringSetVal)
        with self._experiment.lock():
            if not value.values:
                self._enqueue_operation(ClearStringSet(self._path), wait=wait)
            else:
                self._enqueue_operation(ClearStringSet(self._path), wait=False)
                self._enqueue_operation(AddStrings(self._path, value.values), wait=wait)

    def add(self, values: Union[str, Iterable[str]], wait: bool = False):
        values = self._to_proper_value_type(values)
        with self._experiment.lock():
            self._enqueue_operation(AddStrings(self._path, set(values)), wait)

    def remove(self, values: Union[str, Iterable[str]], wait: bool = False):
        values = self._to_proper_value_type(values)
        with self._experiment.lock():
            self._enqueue_operation(RemoveStrings(self._path, set(values)), wait)

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(ClearStringSet(self._path), wait)

    def get(self, wait=True) -> typing.Set[str]:
        # pylint: disable=protected-access
        if wait:
            self._experiment.wait()
        val = self._api_client.get_string_set_attribute(self._experiment_uuid, self._path)
        return val.values

    @staticmethod
    def _to_proper_value_type(values: Union[str, Iterable[str]]) -> Iterable[str]:
        if is_collection(values):
            verify_collection_type("values", values, str)
            return list(values)
        else:
            verify_type("values", values, str)
            return [values]
