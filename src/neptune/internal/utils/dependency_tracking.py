__all__ = [
    "DependencyTrackingStrategy",
    "InferDependenciesStrategy",
    "FileDependenciesStrategy",
]

import os
import subprocess
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Union,
)

from neptune.types import File

if TYPE_CHECKING:
    from neptune import Run


class DependencyTrackingStrategy(ABC):
    @abstractmethod
    def log_dependencies(self, run: "Run") -> None:
        ...


class InferDependenciesStrategy(DependencyTrackingStrategy):
    def log_dependencies(self, run: "Run") -> None:
        try:
            dependencies_str = subprocess.check_output(["pipreqs", "--print", "."]).decode("utf-8")
        except subprocess.SubprocessError:
            return

        if dependencies_str:
            run["source_code/requirements"].upload(File.from_content(dependencies_str))


class FileDependenciesStrategy(DependencyTrackingStrategy):
    def __init__(self, path: Union[str, os.PathLike]):
        self._path = path

    def log_dependencies(self, run: "Run") -> None:
        if os.path.isfile(self._path):
            run["source_code/files"].upload_files(os.path.basename(self._path))
