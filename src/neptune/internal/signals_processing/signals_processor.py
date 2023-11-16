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
__all__ = ["SignalsProcessor"]

from queue import (
    Empty,
    Queue,
)
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
)

from neptune.internal.signals_processing.signals import SignalsVisitor
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.internal.signals_processing.signals import Signal
    from neptune.metadata_containers import MetadataContainer


class SignalsProcessor(Daemon, SignalsVisitor):
    def __init__(
        self,
        *,
        period: float,
        container: "MetadataContainer",
        queue: "Queue[Signal]",
        async_lag_threshold: float,
        async_no_progress_threshold: float,
        async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = None,
        async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = None,
    ) -> None:
        super().__init__(sleep_time=period, name="CallbacksMonitor")

        self._container: "MetadataContainer" = container
        self._queue: "Queue[Signal]" = queue
        self._async_lag_threshold: float = async_lag_threshold
        self._async_no_progress_threshold: float = async_no_progress_threshold
        self._async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = async_lag_callback
        self._async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = async_no_progress_callback

        self._last_operation_queued: float = 0

    def visit_operation_queued(self, signal: "Signal") -> None:
        self._last_operation_queued = signal.occured_at
        logger.info("Operation queued")

    def work(self) -> None:
        try:
            while not self._queue.empty():
                signal = self._queue.get_nowait()
                signal.accept(self)
        except Empty:
            pass
