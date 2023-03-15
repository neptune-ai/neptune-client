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
__all__ = ["to_git_info", "GitInfo"]

import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import (
    List,
    Optional,
    Union,
)

from neptune.types.atoms.git_ref import (
    GitRef,
    GitRefDisabled,
)

_logger = logging.getLogger(__name__)


@dataclass
class GitInfo:
    commit_id: str
    message: str
    author_name: str
    author_email: str
    commit_date: datetime
    dirty: bool
    branch: Optional[str]
    remotes: Optional[List[str]]


def get_git_repo(repo_path):
    # WARN: GitPython asserts the existence of `git` executable
    # which consists in failure during the preparation of conda package
    try:
        import git

        return git.Repo(repo_path, search_parent_directories=True)
    except ImportError:
        warnings.warn("GitPython could not be initialized")


def to_git_info(git_ref: Union[GitRef, GitRefDisabled]) -> Optional[GitInfo]:
    try:
        if git_ref == GitRef.DISABLED:
            return None

        initial_repo_path = git_ref.resolve_path()
        if initial_repo_path is None:
            return None

        repo = get_git_repo(repo_path=initial_repo_path)
        commit = repo.head.commit

        active_branch = ""

        try:
            active_branch = repo.active_branch.name
        except TypeError as e:
            if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
                active_branch = "Detached HEAD"

        remote_urls = [remote.url for remote in repo.remotes]

        return GitInfo(
            commit_id=commit.hexsha,
            message=commit.message,
            author_name=commit.author.name,
            author_email=commit.author.email,
            commit_date=commit.committed_datetime,
            dirty=repo.is_dirty(untracked_files=True),
            branch=active_branch,
            remotes=remote_urls,
        )
    except:  # noqa: E722
        return None
