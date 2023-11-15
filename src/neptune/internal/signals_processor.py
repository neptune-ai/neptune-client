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
from typing import (
    Optional,
    TYPE_CHECKING,
)
from queue import Queue

from neptune.internal.background_job import BackgroundJob
from neptune.internal.threading.daemon import Daemon

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer


class CallbacksMonitor(BackgroundJob):
    def __init__(self, queue: Queue, period: float = 10) -> None:
        self._queue: "Queue" = queue
        self._period: float = period
        self._thread: Optional["CallbacksMonitor.SignalsProcessor"] = None
        self._started = False

    def start(self, container: "MetadataContainer") -> None:
        self._thread = CallbacksMonitor.SignalsProcessor(period=self._period, container=container, queue=self._queue)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started and self._thread is None:
            return
        self._thread.interrupt()

    def join(self, seconds: Optional[float] = None) -> None:
        if not self._started and self._thread is None:
            return
        self._thread.join(seconds)

    def pause(self) -> None:
        if self._thread:
            self._thread.pause()

    def resume(self) -> None:
        if self._thread:
            self._thread.resume()

    class SignalsProcessor(Daemon):
        def __init__(self, period: float, container: "MetadataContainer", queue: "Queue") -> None:
            super().__init__(sleep_time=period, name="CallbacksMonitor")
            self._container: "MetadataContainer" = container
            self._queue: "Queue" = queue

        def work(self) -> None:
            pass
