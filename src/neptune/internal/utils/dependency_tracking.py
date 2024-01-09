#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
    "DependencyTrackingStrategy",
    "InferDependenciesStrategy",
    "FileDependenciesStrategy",
]

import os
import sys
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Union,
)

from neptune.internal.utils.logger import get_logger

if sys.version_info >= (3, 8):
    from importlib.metadata import (
        Distribution,
        distributions,
    )
else:
    from importlib_metadata import Distribution, distributions

from neptune.types import File

if TYPE_CHECKING:
    from neptune import Run

logger = get_logger()


class DependencyTrackingStrategy(ABC):
    @abstractmethod
    def log_dependencies(self, run: "Run") -> None:
        ...


class InferDependenciesStrategy(DependencyTrackingStrategy):
    def log_dependencies(self, run: "Run") -> None:
        dependencies = []

        def sorting_key_func(d: Distribution) -> str:
            _name = d.metadata["Name"]
            return _name.lower() if isinstance(_name, str) else ""

        dists = sorted(distributions(), key=sorting_key_func)

        for dist in dists:
            if dist.metadata["Name"]:
                dependencies.append(f'{dist.metadata["Name"]}=={dist.metadata["Version"]}')

        dependencies_str = "\n".join(dependencies)

        if dependencies_str:
            run["source_code/requirements"].upload(File.from_content(dependencies_str))


class FileDependenciesStrategy(DependencyTrackingStrategy):
    def __init__(self, path: Union[str, os.PathLike]):
        self._path = path

    def log_dependencies(self, run: "Run") -> None:
        if os.path.isfile(self._path):
            run["source_code/requirements"].upload(self._path)
        else:
            logger.error("[ERROR] File '%s' does not exist - skipping dependency file upload.", self._path)
