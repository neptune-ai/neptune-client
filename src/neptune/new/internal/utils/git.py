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

from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Optional,
    Union,
)


@dataclass
class GitInfo:
    repository_path: Optional[Union[str, Path]] = None

    def get_repository(self) -> Optional[Any]:
        if self.repository_path is not None:
            repository_path = Path(self.repository_path).expanduser().resolve()

            # WARN: GitPython asserts the existence of `git` executable
            # which consists in failure during the preparation of conda package
            try:
                from git import Repo

                return Repo(path=repository_path, search_parent_directories=True)
            except Exception:  # noqa
                return None
