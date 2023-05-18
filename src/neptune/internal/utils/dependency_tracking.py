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
from typing import Union

from neptune.types import File


class DependencyTrackingStrategy(ABC):
    @abstractmethod
    def track_dependencies(self) -> None:
        ...


class InferDependenciesStrategy(DependencyTrackingStrategy):
    def __init__(self, run):
        self._run = run

    def track_dependencies(self) -> None:
        try:
            dependencies_str = subprocess.check_output(["pipreqs", "--print", "."]).decode("utf-8")
        except subprocess.SubprocessError:
            return

        if dependencies_str:
            self._run["source_code/requirements"].upload(File.from_content(dependencies_str))


class FileDependenciesStrategy(DependencyTrackingStrategy):
    def __init__(self, run, path: Union[str, os.PathLike]):
        self._run = run
        self._path = path

    def track_dependencies(self) -> None:
        if not os.path.isfile(self._path):
            return

        self._run["source_code/files"].upload_files(self._path)
