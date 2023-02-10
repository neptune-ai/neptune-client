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
__all__ = ["get_git_info", "discover_git_repo_location"]

import logging
import os
import warnings
from typing import Optional

from neptune.new.types.atoms import GitRef
from neptune.vendor.lib_programname import get_path_executed_script

_logger = logging.getLogger(__name__)


def get_git_repo(repo_path):
    # WARN: GitPython asserts the existence of `git` executable
    # which consists in failure during the preparation of conda package
    try:
        import git

        return git.Repo(repo_path, search_parent_directories=True)
    except ImportError:
        warnings.warn("GitPython could not be initialized")


def get_git_info(repo_path=None):
    try:
        repo = get_git_repo(repo_path)

        commit = repo.head.commit

        active_branch = ""

        try:
            active_branch = repo.active_branch.name
        except TypeError as e:
            if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
                active_branch = "Detached HEAD"

        remote_urls = [remote.url for remote in repo.remotes]

        return GitRef(
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


def get_git_repo_path(initial_path: str) -> Optional[str]:
    try:
        return get_git_repo(initial_path).git_dir
    except:  # noqa: E722
        pass


def discover_git_repo_location() -> Optional[str]:
    potential_initial_path = os.path.dirname(os.path.abspath(get_path_executed_script()))
    return get_git_repo_path(initial_path=potential_initial_path)
