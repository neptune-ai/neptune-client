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
__all__ = ["ContainerStructure"]

from collections import deque
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.utils.paths import path_to_str

T = TypeVar("T")
Node = TypeVar("Node")


def _default_node_factory(path):
    return {}


class ContainerStructure(Generic[T, Node]):
    def __init__(self, node_factory: Optional[Callable[[List[str]], Node]] = None):
        if node_factory is None:
            node_factory = _default_node_factory

        self._structure = node_factory(path=[])
        self._node_factory = node_factory
        self._node_type = type(self._structure)

    def get_structure(self) -> Node:
        return self._structure

    def _iterate_node(self, node, path_prefix: List[str]):
        """this iterates in BFS order in order to more meaningful suggestions before cutoff"""
        nodes_queue = deque([(node, path_prefix)])
        while nodes_queue:
            node, prefix = nodes_queue.popleft()
            for key, value in node.items():
                if not isinstance(value, self._node_type):
                    yield prefix + [key]
                else:
                    nodes_queue.append((value, prefix + [key]))

    def iterate_subpaths(self, path_prefix: List[str]):
        root = self.get(path_prefix)
        for path in self._iterate_node(root or {}, path_prefix):
            yield path_to_str(path)

    def get(self, path: List[str]) -> Union[T, Node, None]:
        ref = self._structure

        for index, part in enumerate(path):
            if not isinstance(ref, self._node_type):
                raise MetadataInconsistency(
                    "Cannot access path '{}': '{}' is already defined as an attribute, "
                    "not a namespace".format(path_to_str(path), path_to_str(path[:index]))
                )
            if part not in ref:
                return None
            ref = ref[part]

        return ref

    def set(self, path: List[str], attr: T) -> None:
        ref = self._structure
        location, attribute_name = path[:-1], path[-1]

        for idx, part in enumerate(location):
            if part not in ref:
                ref[part] = self._node_factory(location[: idx + 1])
            ref = ref[part]
            if not isinstance(ref, self._node_type):
                raise MetadataInconsistency(
                    "Cannot access path '{}': '{}' is already defined as an attribute, "
                    "not a namespace".format(path_to_str(path), part)
                )

        if attribute_name in ref and isinstance(ref[attribute_name], self._node_type):
            if isinstance(attr, self._node_type):
                # in-between nodes are auto-created, so ignore it's OK unless we want to change the type
                return
            raise MetadataInconsistency("Cannot set attribute '{}'. It's a namespace".format(path_to_str(path)))

        ref[attribute_name] = attr

    def pop(self, path: List[str]) -> None:
        self._pop_impl(self._structure, path, path)

    def _pop_impl(self, ref, sub_path: List[str], attr_path: List[str]):
        if not sub_path:
            return

        head, tail = sub_path[0], sub_path[1:]
        if head not in ref:
            raise MetadataInconsistency("Cannot delete {}. Attribute not found.".format(path_to_str(attr_path)))

        if not tail:
            if isinstance(ref[head], self._node_type):
                raise MetadataInconsistency(
                    "Cannot delete {}. It's a namespace, not an attribute.".format(path_to_str(attr_path))
                )
            del ref[head]
        else:
            self._pop_impl(ref[head], tail, attr_path)
            if not ref[head]:
                del ref[head]

    def clear(self):
        self._structure.clear()
