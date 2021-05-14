#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import logging

from typing import TYPE_CHECKING, Optional

from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.threading.daemon import Daemon

if TYPE_CHECKING:
    from neptune.new.run import Run

_logger = logging.getLogger(__name__)


class PingBackgroundJob(BackgroundJob):

    def __init__(self, period: float = 10):
        self._period = period
        self._thread = None
        self._started = False

    def start(self, run: 'Run'):
        self._thread = self.ReportingThread(self._period, run)
        self._thread.start()
        self._started = True

    def stop(self):
        if not self._started:
            return
        self._thread.interrupt()

    def join(self, seconds: Optional[float] = None):
        if not self._started:
            return
        self._thread.join(seconds)

    class ReportingThread(Daemon):

        def __init__(
                self,
                period: float,
                run: 'Run'):
            super().__init__(sleep_time=period)
            self._run = run

        @Daemon.ConnectionRetryWrapper(
            kill_message=(
                "Killing Neptune ping thread. Your run's status will not be updated and"
                " the run will be shown as inactive."
            )
        )
        def work(self) -> None:
            self._run.ping()
