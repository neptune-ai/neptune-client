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
from threading import RLock
from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Optional,
    Union,
)

from neptune.attributes.constants import (
    DIFF_HEAD_INDEX_PATH,
    UPSTREAM_INDEX_DIFF,
)
from neptune.types import File
from neptune.types.atoms.git_ref import (
    GitRef,
    GitRefDisabled,
)

if TYPE_CHECKING:
    import git

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
        from git import (
            InvalidGitRepositoryError,
            NoSuchPathError,
        )
    except ImportError:
        warnings.warn("GitPython could not be initialized")
        return

    try:
        return git.Repo(repo_path, search_parent_directories=True)
    except (NoSuchPathError, InvalidGitRepositoryError):
        return


def to_git_info(git_ref: Union[GitRef, GitRefDisabled]) -> Optional[GitInfo]:
    try:
        repo = ThreadSafeRepo(git_ref)
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


@dataclass
class UncommittedChanges:
    diff_head: Optional[str]
    diff_upstream: Optional[str]
    upstream_sha: Optional[str]


class ThreadSafeMethod:
    def __init__(self, lock: RLock, method: Callable):
        self._lock = lock
        self.method = method

    def __call__(self, *args, **kwargs):
        self._lock.acquire()
        res = self.method(*args, **kwargs)
        self._lock.release()

        return res


class ThreadSafeGit:
    def __init__(self, lock: RLock, git):
        self._lock = lock
        self._git = git

    def __getattr__(self, item: str):
        return ThreadSafeMethod(self._lock, self._git.__getattr__(item))


class ThreadSafeRepo:
    def __init__(self, git_ref: Union[GitRef, GitRefDisabled]):
        self.git_ref = git_ref

        self._lock = RLock()

        self._repo = None
        self.git = None

        self.get_repo_from_git_ref()

    def get_repo_from_git_ref(self) -> None:
        if self.git_ref == GitRef.DISABLED:
            return

        initial_repo_path = self.git_ref.resolve_path()
        if initial_repo_path is None:
            return

        self._repo = get_git_repo(repo_path=initial_repo_path)
        self.git = ThreadSafeGit(self._lock, self._repo.git)

    def __getattr__(self, item):
        original_attribute = self._repo.__getattribute__(item)

        if callable(original_attribute):
            return ThreadSafeMethod(self._lock, self._repo.__getattribute__(item))

        return original_attribute


def get_diff(repo: ThreadSafeRepo, commit_ref: str) -> Optional[str]:
    try:
        from git.exc import GitCommandError

        try:
            return repo.git.diff(commit_ref)
        except GitCommandError:
            return
    except ImportError:
        return None


def get_relevant_upstream_commit(repo: ThreadSafeRepo) -> Optional["git.Commit"]:
    try:
        tracking_branch = repo.active_branch.tracking_branch()
    except (TypeError, ValueError):
        return

    if tracking_branch:
        return tracking_branch.commit

    return search_for_most_recent_ancestor(repo)


def search_for_most_recent_ancestor(repo: ThreadSafeRepo) -> Optional["git.Commit"]:
    most_recent_ancestor: Optional["git.Commit"] = None

    try:
        from git.exc import GitCommandError

        try:
            for branch in repo.heads:
                tracking_branch = branch.tracking_branch()
                if tracking_branch:
                    for ancestor in repo.merge_base(repo.head, tracking_branch.commit):
                        if not most_recent_ancestor or repo.is_ancestor(most_recent_ancestor, ancestor):
                            most_recent_ancestor = ancestor
        except GitCommandError:
            pass
    except ImportError:
        return None

    return most_recent_ancestor


def get_upstream_index_sha(repo: ThreadSafeRepo) -> Optional[str]:
    upstream_commit = get_relevant_upstream_commit(repo)

    if upstream_commit and upstream_commit != repo.head.commit:

        return upstream_commit.hexsha


def get_uncommitted_changes(repo: ThreadSafeRepo) -> Optional[UncommittedChanges]:
    if not repo.is_dirty(untracked_files=True):
        return

    head_index_diff = get_diff(repo, repo.head.name)

    upstream_sha = get_upstream_index_sha(repo)

    upstream_index_diff = get_diff(repo, upstream_sha)

    return UncommittedChanges(head_index_diff, upstream_index_diff, upstream_sha)


def track_uncommitted_changes(git_ref: Union[GitRef, GitRefDisabled], run: "Run") -> None:
    repo = ThreadSafeRepo(git_ref)

    if not repo:
        return

    uncommitted_changes = get_uncommitted_changes(repo)

    if not uncommitted_changes:
        return

    if uncommitted_changes.diff_head:
        run[DIFF_HEAD_INDEX_PATH].upload(File.from_content(uncommitted_changes.diff_head, extension="patch"))

    if uncommitted_changes.diff_upstream and uncommitted_changes.upstream_sha:
        run[f"{UPSTREAM_INDEX_DIFF}{uncommitted_changes.upstream_sha}"].upload(
            File.from_content(uncommitted_changes.diff_upstream, extension="patch")
        )
