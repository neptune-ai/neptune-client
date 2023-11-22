#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["ensure_disk_not_overutilize"]


import os
from abc import (
    ABC,
    abstractmethod,
)
from functools import wraps
from typing import (
    Any,
    Callable,
    Optional,
)

import psutil
from psutil import Error

from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.envs import (
    NEPTUNE_MAX_DISK_USAGE,
    NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED,
)
from neptune.exceptions import NeptuneMaxDiskUtilizationExceeded


def get_neptune_data_directory() -> str:
    return os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY)


def get_disk_utilization_percent(path: Optional[str] = None) -> Optional[float]:
    try:
        if path is None:
            path = get_neptune_data_directory()

        return float(psutil.disk_usage(path).percent)
    except (ValueError, TypeError, Error):
        return None


def get_max_disk_utilization_from_env() -> Optional[float]:
    env_limit_disk_utilization = os.getenv(NEPTUNE_MAX_DISK_USAGE)

    if env_limit_disk_utilization is None:
        return None

    try:
        limit_disk_utilization = float(env_limit_disk_utilization)
        if limit_disk_utilization <= 0 or limit_disk_utilization > 100:
            raise ValueError

        return limit_disk_utilization
    except (ValueError, TypeError):
        warn_once(
            f"Provided invalid value of '{NEPTUNE_MAX_DISK_USAGE}': '{env_limit_disk_utilization}'. "
            "Check of disk utilization will not be applied.",
            exception=NeptuneWarning,
        )
        return None


class DiskUtilizationErrorHandlerTemplate(ABC):
    def __init__(self, max_disk_utilization: Optional[float], func: Callable[..., None], *args: Any, **kwargs: Any):
        self.max_disk_utilization = max_disk_utilization
        self.func = func
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def handle_limit_not_set(self) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def handle_utilization_calculation_error(self) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def handle_limit_not_exceeded(self) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def handle_limit_exceeded(self, current_utilization: float) -> None:
        ...  # pragma: no cover

    def run(self) -> None:
        if not self.max_disk_utilization:
            return self.handle_limit_not_set()

        current_utilization = get_disk_utilization_percent()

        if current_utilization is None:
            return self.handle_utilization_calculation_error()

        if current_utilization < self.max_disk_utilization:
            return self.handle_limit_not_exceeded()

        self.handle_limit_exceeded(current_utilization)


class NonRaisingErrorHandler(DiskUtilizationErrorHandlerTemplate):
    DISK_ISSUE_MSG = "Encountered disk issue. Neptune will not save your data."

    def handle_limit_not_set(self) -> None:
        try:
            return self.func(*self.args, **self.kwargs)
        except (OSError, Error):
            warn_once(self.DISK_ISSUE_MSG, exception=NeptuneWarning)

    def handle_utilization_calculation_error(self) -> None:
        try:
            return self.func(*self.args, **self.kwargs)
        except (OSError, Error):
            warn_once(self.DISK_ISSUE_MSG, exception=NeptuneWarning)

    def handle_limit_not_exceeded(self) -> None:
        try:
            return self.func(*self.args, **self.kwargs)
        except (OSError, Error):
            warn_once(self.DISK_ISSUE_MSG, exception=NeptuneWarning)

    def handle_limit_exceeded(self, current_utilization: float) -> None:
        warn_once(
            f"Disk usage is at {current_utilization}%, which exceeds the maximum allowed utilization "
            + f"of {self.max_disk_utilization}%. Neptune will not save your data.",
            exception=NeptuneWarning,
        )


class RaisingErrorHandler(DiskUtilizationErrorHandlerTemplate):
    def handle_limit_not_set(self) -> None:
        return self.func(*self.args, **self.kwargs)

    def handle_utilization_calculation_error(self) -> None:
        return self.func(*self.args, **self.kwargs)

    def handle_limit_not_exceeded(self) -> None:
        return self.func(*self.args, **self.kwargs)

    def handle_limit_exceeded(self, current_utilization: float) -> None:
        if isinstance(self.max_disk_utilization, float):

            raise NeptuneMaxDiskUtilizationExceeded(
                disk_utilization=current_utilization,
                utilization_limit=self.max_disk_utilization,
            )


def ensure_disk_not_overutilize(func: Callable[..., None]) -> Callable[..., None]:
    raising_on_disk_issue = os.getenv(NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED, "True").lower() in ("true", "t", "1")
    max_disk_utilization = get_max_disk_utilization_from_env()

    error_handler = RaisingErrorHandler if raising_on_disk_issue else NonRaisingErrorHandler

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        error_handler(max_disk_utilization, func, *args, **kwargs).run()

    return wrapper
