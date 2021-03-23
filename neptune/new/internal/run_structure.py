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
from typing import Dict, Any, Optional, List, TypeVar, Generic

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.utils.paths import path_to_str

T = TypeVar('T')


class RunStructure(Generic[T]):

    def __init__(self):
        self._structure = dict()

    def get_structure(self) -> Dict[str, Any]:
        return self._structure

    def get(self, path: List[str]) -> Optional[T]:
        ref = self._structure

        for index, part in enumerate(path):
            if not isinstance(ref, dict):
                raise MetadataInconsistency("Cannot access path '{}': '{}' is already defined as an attribute, "
                                            "not a namespace".format(path_to_str(path), path_to_str(path[:index])))
            if part not in ref:
                return None
            ref = ref[part]

        if isinstance(ref, dict):
            raise MetadataInconsistency("Cannot get attribute '{}'. It's a namespace".format(path_to_str(path)))

        return ref

    def set(self, path: List[str], attr: T) -> None:
        ref = self._structure
        location, attribute_name = path[:-1], path[-1]

        for part in location:
            if part not in ref:
                ref[part] = {}
            ref = ref[part]
            if not isinstance(ref, dict):
                raise MetadataInconsistency("Cannot access path '{}': '{}' is already defined as an attribute, "
                                            "not a namespace".format(path_to_str(path), part))

        if attribute_name in ref and isinstance(ref[attribute_name], dict):
            raise MetadataInconsistency("Cannot set attribute '{}'. It's a namespace".format(path_to_str(path)))

        ref[attribute_name] = attr

    def pop(self, path: List[str]) -> None:
        self._pop_impl(self._structure, path, path)

    def _pop_impl(self, ref: dict, sub_path: List[str], attr_path: List[str]):
        if not sub_path:
            return

        head, tail = sub_path[0], sub_path[1:]
        if head not in ref:
            raise MetadataInconsistency("Cannot delete {}. Attribute not found.".format(path_to_str(attr_path)))

        if not tail:
            if isinstance(ref[head], dict):
                raise MetadataInconsistency(
                    "Cannot delete {}. It's a namespace, not an attribute.".format(path_to_str(attr_path)))
            del ref[head]
        else:
            self._pop_impl(ref[head], tail, attr_path)
            if not ref[head]:
                del ref[head]

    def clear(self):
        self._structure.clear()
