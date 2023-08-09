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
    from importlib.metadata import distributions
else:
    from importlib_metadata import distributions

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
        dists = list(sorted(distributions(), key=lambda d: d.metadata["Name"]))

        for dist in dists:
            name, version = dist.metadata["Name"], dist.metadata["Version"]
            dependencies.append(f"{name}=={version}")

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
