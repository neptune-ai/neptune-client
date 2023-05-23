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
__all__ = ["to_git_info", "GitInfo", "DiffTracker"]

import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import (
    List,
    Optional,
    Union,
)

import git
from git.exc import GitCommandError

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


class DiffTracker:
    def __init__(self, git_ref: Union[GitRef, GitRefDisabled]):
        self.git_ref = git_ref

        initial_repo_path = self.git_ref.resolve_path() if self.git_ref != GitRef.DISABLED else None

        self.repo = get_git_repo(repo_path=initial_repo_path) if initial_repo_path else None

        self.head = self.repo.head

        self._upstream_commit_sha = None

    def get_head_index_diff(self) -> Optional[str]:
        if not self.repo or not self.repo.is_dirty():
            return

        try:
            return self.repo.git.diff(self.head.name)
        except GitCommandError:
            return

    def get_upstream_index_diff(self) -> Optional[str]:
        if not self.repo or not self.repo.is_dirty():
            return

        upstream_commit = self._get_relevant_upstream_commit()

        if upstream_commit and upstream_commit != self.head.commit:

            self._upstream_commit_sha = upstream_commit.hexsha

            try:
                return self.repo.git.diff(upstream_commit.hexsha)
            except GitCommandError:
                return

    def _get_relevant_upstream_commit(self) -> Optional[git.Commit]:
        try:
            tracking_branch = self.repo.active_branch.tracking_branch()
        except (TypeError, ValueError):
            return

        if tracking_branch:
            return tracking_branch.commit

        return self._search_for_most_recent_ancestor()

    def _search_for_most_recent_ancestor(self) -> Optional[git.Commit]:
        most_recent_ancestor: Optional[git.Commit] = None

        try:
            for branch in self.repo.branches:
                tracking_branch = branch.tracking_branch()
                if tracking_branch:
                    for ancestor in self.repo.merge_base(self.head, tracking_branch.commit):
                        if not most_recent_ancestor or self.repo.is_ancestor(most_recent_ancestor, ancestor):
                            most_recent_ancestor = ancestor
        except GitCommandError:
            pass

        return most_recent_ancestor
