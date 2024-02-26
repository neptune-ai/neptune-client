#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["PingBackgroundJob"]

from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune.internal.background_job import BackgroundJob
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils.logger import get_logger

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer

_logger = get_logger()


class PingBackgroundJob(BackgroundJob):
    def __init__(self, period: float = 10):
        self._period = period
        self._thread: PingBackgroundJob.ReportingThread = None
        self._started = False

    def start(self, container: "MetadataContainer"):
        self._thread = self.ReportingThread(self._period, container)
        self._thread.start()
        self._started = True

    def stop(self):
        if not self._started:
            return
        self._thread.interrupt()

    def pause(self):
        self._thread.pause()

    def resume(self):
        self._thread.resume()

    def join(self, seconds: Optional[float] = None):
        if not self._started:
            return
        self._thread.join(seconds)

    class ReportingThread(Daemon):
        def __init__(self, period: float, container: "MetadataContainer"):
            super().__init__(sleep_time=period, name="NeptunePing")
            self._container = container

        @Daemon.ConnectionRetryWrapper(
            kill_message=(
                "Killing Neptune ping thread. Your run's status will not be updated and"
                " the run will be shown as inactive."
            )
        )
        def work(self) -> None:
            self._container.ping()
