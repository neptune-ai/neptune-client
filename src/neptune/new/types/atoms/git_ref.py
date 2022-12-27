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
__all__ = ["GitRef", "NoRepository"]

from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    TypeVar,
)

from neptune.new.internal.utils.git import GitInfo
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class GitRef(Atom):
    commit_id: str
    message: str
    author_name: str
    author_email: str
    commit_date: datetime
    dirty: bool
    branch: Optional[str]
    remotes: Optional[List[str]]

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_git_ref(self)

    def __str__(self):
        return "GitRef({})".format(str(self.commit_id))

    @staticmethod
    def from_git_info(git_info: GitInfo) -> Optional["GitRef"]:
        repository = git_info.get_repository()

        if repository is not None:
            commit = repository.head.commit
            active_branch = get_active_branch(repository=repository)
            remote_urls = [remote.url for remote in repository.remotes]

            return GitRef(
                commit_id=commit.hexsha,
                message=commit.message,
                author_name=commit.author.name,
                author_email=commit.author.email,
                commit_date=commit.committed_datetime,
                dirty=repository.is_dirty(untracked_files=True),
                branch=active_branch,
                remotes=remote_urls,
            )


def get_active_branch(repository) -> str:
    try:
        return repository.active_branch.name
    except TypeError as e:
        if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
            return "Detached HEAD"


NoRepository = GitInfo()
