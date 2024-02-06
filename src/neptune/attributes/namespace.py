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
__all__ = ["Namespace", "NamespaceBuilder"]

import argparse
from collections.abc import MutableMapping
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
)

from neptune.attributes.attribute import Attribute
from neptune.internal.container_structure import ContainerStructure
from neptune.internal.utils.generic_attribute_mapper import (
    NoValue,
    atomic_attribute_types_map,
)
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.paths import (
    parse_path,
    path_to_str,
)
from neptune.types.namespace import Namespace as NamespaceVal

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer

logger = get_logger()
RunStructure = ContainerStructure  # backwards compatibility


class Namespace(Attribute, MutableMapping):
    def __init__(self, container: "MetadataContainer", path: List[str]):
        Attribute.__init__(self, container, path)
        self._attributes = {}
        self._str_path = path_to_str(path)

    def __setitem__(self, k: str, v: Attribute) -> None:
        if not parse_path(k):
            logger.warning(
                f'Key "{k}" can\'t be used in Namespaces and dicts stored in Neptune. Please use a non-empty key '
                f"instead. The value {v!r} will be dropped.",
            )
            return
        self._attributes[k] = v

    def __delitem__(self, k: str) -> None:
        del self._attributes[k]

    def __getitem__(self, k: str) -> Attribute:
        return self._attributes[k]

    def __len__(self) -> int:
        return len(self._attributes)

    def __iter__(self) -> Iterator[str]:
        yield from self._attributes.__iter__()

    def extend(
        self,
        value: Union[Any, Iterable[Any]],
        *,
        steps: Optional[Collection[float]] = None,
        timestamps: Optional[Collection[float]] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        if not isinstance(value, NamespaceVal):
            value = NamespaceVal(value)
        for k, v in value.value.items():
            self._container[f"{self._str_path}/{k}"].extend(v, steps=steps, timestamps=timestamps, wait=wait, **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in self._attributes.items():
            if isinstance(value, Namespace):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def assign(self, value: Union[NamespaceVal, dict, Mapping], *, wait: bool = False):
        if isinstance(value, argparse.Namespace):
            value = NamespaceVal(vars(value))
        elif not isinstance(value, NamespaceVal):
            value = NamespaceVal(value)

        for k, v in value.value.items():
            self._container[f"{self._str_path}/{k}"].assign(v, wait=wait)

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
        attributes = self._backend.fetch_atom_attribute_values(self._container_id, self._container_type, self._path)
        run_struct = ContainerStructure()
        prefix_len = len(self._path)
        for attr_name, attr_type, attr_value in attributes:
            run_struct.set(parse_path(attr_name)[prefix_len:], (attr_type, attr_value))
        return self._collect_atom_values(run_struct.get_structure())


class NamespaceBuilder:
    def __init__(self, container: "MetadataContainer"):
        self._run = container

    def __call__(self, path: List[str]) -> Namespace:
        return Namespace(self._run, path)
