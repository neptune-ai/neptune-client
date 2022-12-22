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
__all__ = ["GitInfo"]

import warnings
from pathlib import Path
from typing import (
    Any,
    Optional,
    Union,
)


class GitInfo:
    def __init__(self, *, repository_path: Optional[Union[str, Path]] = ""):
        self._repository_path = repository_path

    @property
    def repository_path(self) -> Optional[Path]:
        if self._repository_path is not None:
            return Path(self._repository_path).parent.resolve()

    def get_repository(self) -> Optional[Any]:
        # WARN: GitPython asserts the existence of `git` executable
        # which consists in failure during the preparation of conda package
        try:
            from git import Repo

            return Repo(path=self.repository_path, search_parent_directories=True)
        except ImportError:
            warnings.warn("GitPython could not be initialized")


NoRepository = GitInfo(repository_path=None)
