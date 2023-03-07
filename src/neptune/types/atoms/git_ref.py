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
__all__ = ["GitRef", "GitRefDisabled"]

from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
    Union,
)

from neptune.types.atoms.atom import Atom
from neptune.vendor.lib_programname import get_path_executed_script

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class GitRef(Atom):
    """
    Represents a git repository metadata.
    args:
        repository_path: path to the repository. If not provided, the path to the script that is currently executed
    """

    repository_path: Optional[Union[str, Path]] = get_path_executed_script()

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_git_ref(self)

    def __str__(self) -> str:
        return f"GitRef({self.repository_path})"

    def resolve_path(self) -> Optional[Path]:
        if self.repository_path is None:
            return None
        return Path(self.repository_path).resolve()

    @classmethod
    def disabled(cls) -> "GitRef":
        """
        Returns `GitRef` that do not track git repository metadata.
        """
        return cls(repository_path=None)


GitRefDisabled = GitRef.disabled()
"""
`GitRef` that do not track git repository metadata.
"""
