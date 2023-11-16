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

from queue import Queue
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
)

from neptune.internal.background_job import BackgroundJob
from neptune.internal.signals_processing.signals_processor import SignalsProcessor

if TYPE_CHECKING:
    from neptune.internal.signals_processing.signals import Signal
    from neptune.metadata_containers import MetadataContainer


class CallbacksMonitor(BackgroundJob):
    def __init__(
        self,
        queue: "Queue[Signal]",
        async_lag_threshold: float,
        async_no_progress_threshold: float,
        async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = None,
        async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = None,
        period: float = 10,
    ) -> None:
        self._period: float = period
        self._queue: "Queue[Signal]" = queue
        self._thread: Optional["SignalsProcessor"] = None
        self._started: bool = False
        self._async_lag_threshold: float = async_lag_threshold
        self._async_no_progress_threshold: float = async_no_progress_threshold
        self._async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = async_lag_callback
        self._async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = async_no_progress_callback

    def start(self, container: "MetadataContainer") -> None:
        self._thread = SignalsProcessor(
            period=self._period,
            container=container,
            queue=self._queue,
            async_lag_threshold=self._async_lag_threshold,
            async_no_progress_threshold=self._async_no_progress_threshold,
            async_lag_callback=self._async_lag_callback,
            async_no_progress_callback=self._async_no_progress_callback,
        )
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if self._thread and self._started:
            self._thread.interrupt()

    def join(self, seconds: Optional[float] = None) -> None:
        if self._thread and self._started:
            self._thread.join(seconds)

    def pause(self) -> None:
        if self._thread:
            self._thread.pause()

    def resume(self) -> None:
        if self._thread:
            self._thread.resume()
