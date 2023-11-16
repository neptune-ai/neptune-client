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
from threading import Thread
from time import monotonic
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
)

from neptune.internal.init.parameters import IN_BETWEEN_CALLBACKS_MINIMUM_INTERVAL
from neptune.internal.signals_processing.signals import SignalsVisitor
from neptune.internal.threading.daemon import Daemon

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
        callbacks_interval: float = IN_BETWEEN_CALLBACKS_MINIMUM_INTERVAL,
    ) -> None:
        super().__init__(sleep_time=period, name="CallbacksMonitor")

        self._container: "MetadataContainer" = container
        self._queue: "Queue[Signal]" = queue
        self._async_lag_threshold: float = async_lag_threshold
        self._async_no_progress_threshold: float = async_no_progress_threshold
        self._async_lag_callback: Optional[Callable[["MetadataContainer"], None]] = async_lag_callback
        self._async_no_progress_callback: Optional[Callable[["MetadataContainer"], None]] = async_no_progress_callback

        self._last_batch_started_at: Optional[float] = None
        self._last_no_progress_callback_at: Optional[float] = None

    def visit_batch_started(self, signal: "Signal") -> None:
        if self._last_batch_started_at is None:
            self._last_batch_started_at = signal.occured_at

    def visit_batch_processed(self, signal: "Signal") -> None:
        if self._last_batch_started_at is not None:
            self._last_batch_started_at = None

    def _check_callbacks(self) -> None:
        self._check_no_progress()

    def _check_no_progress(self) -> None:
        if self._last_batch_started_at is not None:
            if monotonic() - self._last_batch_started_at > self._async_no_progress_threshold:
                if self._async_no_progress_callback is not None:
                    if (
                        self._last_no_progress_callback_at is None
                        or monotonic() - self._last_no_progress_callback_at > IN_BETWEEN_CALLBACKS_MINIMUM_INTERVAL
                    ):
                        execute_in_async(callback=self._async_no_progress_callback, container=self._container)
                    self._last_no_progress_callback_at = monotonic()

    def work(self) -> None:
        try:
            while not self._queue.empty():
                signal = self._queue.get_nowait()
                signal.accept(self)
            self._check_callbacks()
        except Empty:
            pass


def execute_in_async(*, callback: Callable[["MetadataContainer"], None], container: "MetadataContainer") -> None:
    Thread(target=callback, name="CallbackExecution", args=(container,), daemon=True).start()
