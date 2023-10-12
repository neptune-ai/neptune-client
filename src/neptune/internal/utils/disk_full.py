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
__all__ = ["ensure_disk_not_full"]


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


def get_disk_utilization_percent(path: Optional[str] = None) -> float:
    try:
        if path is None:
            path = get_neptune_data_directory()

        return float(psutil.disk_usage(path).percent)
    except (ValueError, UnicodeEncodeError):
        return 0


def get_max_percentage_from_env() -> Optional[float]:
    value = os.getenv(NEPTUNE_MAX_DISK_UTILIZATION)
    if value is not None:
        return float(value)
    return None


def ensure_disk_not_full(func: Callable[..., None]) -> Callable[..., None]:
    non_raising_on_disk_issue = NEPTUNE_NON_RAISING_ON_DISK_ISSUE in os.environ
    max_disk_utilization = get_max_percentage_from_env()

    @wraps(func)
    def wrapper(*args: Tuple, **kwargs: Dict[str, Any]) -> None:
        if non_raising_on_disk_issue:
            try:
                if max_disk_utilization:
                    current_utilization = get_disk_utilization_percent()
                    if current_utilization >= max_disk_utilization:
                        warn_once(
                            f"Max disk utilization {max_disk_utilization}% exceeded with {current_utilization}."
                            f" Neptune will not be saving your data.",
                            exception=NeptuneWarning,
                        )
                        return

                func(*args, **kwargs)
            except (OSError, Error):
                warn_once("Encountered disk issue and Neptune will not be saving your data.", exception=NeptuneWarning)
        else:
            return func(*args, **kwargs)

    return wrapper
