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
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
)

import psutil
from psutil import Error

from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.envs import (
    NEPTUNE_MAX_DISK_UTILIZATION,
    NEPTUNE_NON_RAISING_ON_DISK_ISSUE,
)


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
    env_limit_disk_utilization = os.getenv(NEPTUNE_MAX_DISK_UTILIZATION)

    if env_limit_disk_utilization is None:
        return None

    try:
        limit_disk_utilization = float(env_limit_disk_utilization)
        if limit_disk_utilization <= 0 or limit_disk_utilization > 100:
            raise ValueError

        return limit_disk_utilization
    except (ValueError, TypeError):
        warn_once(
            f"Provided invalid value of '{NEPTUNE_MAX_DISK_UTILIZATION}': '{env_limit_disk_utilization}'. "
            "Check of disk utilization will not be applied.",
            exception=NeptuneWarning,
        )
        return None


def ensure_disk_not_overutilize(func: Callable[..., None]) -> Callable[..., None]:
    non_raising_on_disk_issue = os.getenv(NEPTUNE_NON_RAISING_ON_DISK_ISSUE, "false").lower() in ("true", "t", "1")
    max_disk_utilization = get_max_disk_utilization_from_env()

    @wraps(func)
    def wrapper(*args: Tuple, **kwargs: Dict[str, Any]) -> None:
        if non_raising_on_disk_issue:
            try:
                if max_disk_utilization:
                    current_utilization = get_disk_utilization_percent()
                    if current_utilization is None:
                        warn_once(
                            "Encountered disk issue during utilization check. Neptune will not save your data.",
                            exception=NeptuneWarning,
                        )
                        return

                    if current_utilization >= max_disk_utilization:
                        warn_once(
                            f"Disk usage is at {current_utilization}%, which exceeds the maximum allowed utilization "
                            + "of {max_disk_utilization}%. Neptune will not save your data.",
                            exception=NeptuneWarning,
                        )
                        return

                func(*args, **kwargs)
            except (OSError, Error):
                warn_once("Encountered disk issue. Neptune will not save your data.", exception=NeptuneWarning)

        else:
            return func(*args, **kwargs)

    return wrapper
