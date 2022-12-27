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
__all__ = ["Artifact"]

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
)

from neptune.new.internal.artifacts.file_hasher import FileHasher
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class Artifact(Atom):

    hash: str

    def __init__(self, value: Optional[str] = None):
        self.hash = str(value)
        assert (
            len(self.hash) == FileHasher.HASH_LENGTH or value is None
        ), "Expected sha-256 string. E.g. 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'"

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_artifact(self)

    def __str__(self):
        return "Artifact({})".format(self.hash)
