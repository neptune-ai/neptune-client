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

from neptune.exceptions import MetadataInconsistency

T = TypeVar('T')


class ExperimentStructure(Generic[T]):

    def __init__(self):
        self._structure = dict()

    def get_structure(self) -> Dict[str, Any]:
        return self._structure

    def get(self, path: List[str]) -> Optional[T]:
        ref = self._structure

        for part in path:
            if not isinstance(ref, dict):
                raise MetadataInconsistency("Cannot access path '{}': '{}' is already defined as a variable, "
                                            "not a namespace".format(path, part))
            if part not in ref:
                return None
            ref = ref[part]

        if isinstance(ref, dict):
            raise MetadataInconsistency("Cannot get variable '{}'. It's a namespace".format(path))

        return ref

    def set(self, path: List[str], var: T) -> None:
        ref = self._structure
        location, variable_name = path[:-1], path[-1]

        for part in location:
            if part not in ref:
                ref[part] = {}
            ref = ref[part]
            if not isinstance(ref, dict):
                raise MetadataInconsistency("Cannot access path '{}': '{}' is already defined as a variable, "
                                            "not a namespace".format(path, part))

        if variable_name in ref and isinstance(ref[variable_name], dict):
            raise MetadataInconsistency("Cannot set variable '{}'. It's a namespace".format(path))

        ref[variable_name] = var

    def pop(self, path: List[str]) -> None:
        self._pop_impl(self._structure, path, path)

    def _pop_impl(self, ref: dict, sub_path: List[str], var_path: List[str]):
        if not sub_path:
            return

        head, tail = sub_path[0], sub_path[1:]
        if head not in ref:
            raise MetadataInconsistency("Cannot delete {}. Variable not found.".format(var_path))

        if not tail:
            if isinstance(ref[head], dict):
                raise MetadataInconsistency("Cannot delete {}. It's a namespace, not a variable.")
            del ref[head]
        else:
            self._pop_impl(ref[head], tail, var_path)
            if not ref[head]:
                del ref[head]
