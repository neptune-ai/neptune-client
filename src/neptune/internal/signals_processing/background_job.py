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
__all__ = ["CallbacksMonitor"]

from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
)

from neptune.internal.background_job import BackgroundJob
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer


class CallbacksMonitor(BackgroundJob):
    def __init__(
        self,
        async_lag_threshold: float,
        async_no_progress_threshold: float,
        async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = None,
        async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = None,
        period: float = 10,
    ) -> None:
        self._period: float = period
        self._async_lag_threshold: float = async_lag_threshold
        self._async_no_progress_threshold: float = async_no_progress_threshold
        self._async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = async_lag_callback
        self._async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = async_no_progress_callback

        logger.info("CallbacksMonitor is not implemented yet")

    def start(self, container: "MetadataContainer") -> None:
        # TODO: implement
        ...

    def stop(self) -> None:
        # TODO: implement
        ...

    def join(self, seconds: Optional[float] = None) -> None:
        # TODO: implement
        ...

    def pause(self) -> None:
        # TODO: implement
        ...

    def resume(self) -> None:
        # TODO: implement
        ...
