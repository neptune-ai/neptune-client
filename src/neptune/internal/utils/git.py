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
__all__ = [
    "to_git_info",
    "GitInfo",
    "track_uncommitted_changes",
]

import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Union,
)

import git
from git.exc import (
    GitCommandError,
    InvalidGitRepositoryError,
    NoSuchPathError,
)

from neptune.types import File
from neptune.types.atoms.git_ref import (
    GitRef,
    GitRefDisabled,
)

if TYPE_CHECKING:
    from neptune import Run

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
    def __init__(self, repo: git.Repo):
        self._repo = repo
        self._head: git.HEAD = self._repo.head
        self._upstream_commit_sha: Optional[str] = None

    @property
    def repo_dirty(self) -> bool:
        return self._repo.is_dirty()

    @classmethod
    def from_git_ref(cls, git_ref: Union[GitRef, GitRefDisabled]) -> Optional["DiffTracker"]:
        if git_ref == GitRef.DISABLED:
            return None

        initial_repo_path = git_ref.resolve_path()
        if initial_repo_path is None:
            return None
        try:
            repo = get_git_repo(repo_path=initial_repo_path)
        except (NoSuchPathError, InvalidGitRepositoryError):
            return None

        return cls(repo)

    @property
    def upstream_commit_sha(self) -> Optional[str]:
        return self._upstream_commit_sha

    def get_head_index_diff(self) -> Optional[str]:
        try:
            return self._repo.git.diff(self._head.name)
        except GitCommandError:
            return

    def get_upstream_index_diff(self) -> Optional[str]:
        upstream_commit = self._get_relevant_upstream_commit()

        if upstream_commit and upstream_commit != self._head.commit:

            self._upstream_commit_sha = upstream_commit.hexsha

            try:
                return self._repo.git.diff(upstream_commit.hexsha)
            except GitCommandError:
                return

    def _get_relevant_upstream_commit(self) -> Optional[git.Commit]:
        try:
            tracking_branch = self._repo.active_branch.tracking_branch()
        except (TypeError, ValueError):
            return

        if tracking_branch:
            return tracking_branch.commit

        return self._search_for_most_recent_ancestor()

    def _search_for_most_recent_ancestor(self) -> Optional[git.Commit]:
        most_recent_ancestor: Optional[git.Commit] = None

        try:
            for branch in self._repo.branches:
                tracking_branch = branch.tracking_branch()
                if tracking_branch:
                    for ancestor in self._repo.merge_base(self._head, tracking_branch.commit):
                        if not most_recent_ancestor or self._repo.is_ancestor(most_recent_ancestor, ancestor):
                            most_recent_ancestor = ancestor
        except GitCommandError:
            pass

        return most_recent_ancestor


def track_uncommitted_changes(git_ref: Union[GitRef, GitRefDisabled], run: "Run") -> None:
    tracker = DiffTracker.from_git_ref(git_ref)

    if not tracker or not tracker.repo_dirty:
        return

    diff_head = tracker.get_head_index_diff()
    diff_upstream = tracker.get_upstream_index_diff()
    upstream_sha = tracker.upstream_commit_sha

    if diff_head:
        run["source_code/diff"].upload(File.from_content(diff_head, extension="patch"))

    if diff_upstream:
        run[f"source_code/upstream_diff_{upstream_sha}"].upload(File.from_content(diff_upstream, extension="patch"))
