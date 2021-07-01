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
from typing import Any, Dict, TYPE_CHECKING, Iterator, List, Mapping, Union

from neptune.new.attributes.attribute import Attribute
from neptune.new.internal.run_structure import RunStructure
from neptune.new.internal.utils.generic_attribute_mapper import atomic_attribute_types_map, NoValue
from neptune.new.internal.utils.paths import parse_path, path_to_str
from neptune.new.types.namespace import Namespace as NamespaceVal

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

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self._attributes.items():
            if isinstance(value, Namespace):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def assign(self, value: Union[NamespaceVal, dict, Mapping], wait: bool = False):
        if not isinstance(value, NamespaceVal):
            value = NamespaceVal(value)

        for k, v in value.value.items():
            self._run[f"{self._str_path}/{k}"].assign(v, wait)

    def _collect_atom_values(self, attribute_dict) -> dict:
        result = {}
        for k, v in attribute_dict.items():
            if isinstance(v, dict):
                result[k] = self._collect_atom_values(v)
            else:
                attr_type, attr_value = v
                if attr_type in atomic_attribute_types_map and attr_value is not NoValue:
                    result[k] = v[1]
        return result

    def fetch(self) -> dict:
        attributes = self._backend.fetch_atom_attribute_values(self._run_uuid, self._path)
        run_struct = RunStructure()
        prefix_len = len(self._path)
        for attr_name, attr_type, attr_value in attributes:
            run_struct.set(parse_path(attr_name)[prefix_len:], (attr_type, attr_value))
        return self._collect_atom_values(run_struct.get_structure())


class NamespaceBuilder:
    def __init__(self, run: 'Run'):
        self._run = run

    def __call__(self, path: List[str]) -> Namespace:
        return Namespace(self._run, path)
