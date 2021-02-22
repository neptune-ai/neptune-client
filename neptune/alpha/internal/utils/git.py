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
import logging
import os
import sys
from typing import Optional, List

from neptune.alpha import Experiment
from neptune.alpha.attributes import constants as attr_consts
from neptune.alpha.internal.utils import get_absolute_paths, get_common_root
from neptune.alpha.types.atoms import GitRef
from neptune.internal.storage.storage_utils import normalize_file_name
from neptune.utils import is_ipython

_logger = logging.getLogger(__name__)


def get_git_info(repo_path=None):
    try:
        # pylint:disable=bad-option-value,import-outside-toplevel
        import git

        repo = git.Repo(repo_path, search_parent_directories=True)

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
            remotes=remote_urls
        )
    except:  # pylint: disable=bare-except
        return None


def discover_git_repo_location() -> Optional[str]:
    # pylint:disable=bad-option-value,import-outside-toplevel
    import __main__

    if hasattr(__main__, '__file__'):
        return os.path.dirname(os.path.abspath(__main__.__file__))
    return None


def upload_source_code(source_files: Optional[List[str]], experiment: Experiment) -> None:
    if not is_ipython() and os.path.isfile(sys.argv[0]):
        if source_files is None:
            entrypoint = os.path.basename(sys.argv[0])
            source_files = sys.argv[0]
        elif not source_files:
            entrypoint = os.path.basename(sys.argv[0])
        else:
            common_root = get_common_root(get_absolute_paths(source_files))
            if common_root is not None:
                entrypoint = normalize_file_name(os.path.relpath(os.path.abspath(sys.argv[0]), common_root))
            else:
                entrypoint = normalize_file_name(os.path.abspath(sys.argv[0]))
        experiment[attr_consts.SOURCE_CODE_ENTRYPOINT_ATTRIBUTE_PATH] = entrypoint

    if source_files is not None:
        experiment[attr_consts.SOURCE_CODE_FILES_ATTRIBUTE_PATH].save_files(source_files)
