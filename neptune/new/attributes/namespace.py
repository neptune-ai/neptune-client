#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Iterator, List, Mapping, Union

from neptune.new.attributes.attribute import Attribute
from neptune.new.types.namespace import Namespace as NamespaceVal
from neptune.new.internal.utils.paths import path_to_str


if TYPE_CHECKING:
    from neptune.new.run import Run


class Namespace(Attribute, MutableMapping):
    def __init__(self, run: 'Run', path: List[str]):
        Attribute.__init__(self, run, path)
        self._attributes = {}
        self._str_path = path_to_str(path)

    def __setitem__(self, k: str, v: Attribute) -> None:
        self._attributes[k] = v

    def __delitem__(self, k: str) -> None:
        del self._attributes[k]

    def __getitem__(self, k: str) -> Attribute:
        return self._attributes[k]

    def __len__(self) -> int:
        return len(self._attributes)

    def __iter__(self) -> Iterator[str]:
        yield from self._attributes.__iter__()

    def assign(self, value: Union[NamespaceVal, dict, Mapping], wait: bool = False):
        if not isinstance(value, NamespaceVal):
            value = NamespaceVal(value)

        for k, v in value.value.items():
            if k in self:
                self[k].assign(v, wait)
            else:
                self._run.define(f"{self._str_path}/{k}", v)

    def fetch(self) -> dict:
        # pylint: disable=protected-access
        return self._backend.get_namespace_attributes(self._run_uuid, self._path)


class NamespaceBuilder:
    def __init__(self, run: 'Run'):
        self._run = run

    def __call__(self, path: List[str]) -> Namespace:
        return Namespace(self._run, path)
