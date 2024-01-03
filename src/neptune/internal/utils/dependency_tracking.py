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

if sys.version_info >= (3, 8):
    from importlib.metadata import (
        Distribution,
        distributions,
    )
else:
    from importlib_metadata import Distribution, distributions

from neptune.internal.utils.logger import logger
from neptune.types import File

if TYPE_CHECKING:
    from neptune import Run


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
