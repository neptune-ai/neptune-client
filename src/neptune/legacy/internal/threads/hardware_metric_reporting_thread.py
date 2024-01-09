#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import time

from bravado.exception import HTTPError

from neptune.legacy.exceptions import NeptuneException
from neptune.legacy.internal.threads.neptune_thread import NeptuneThread

_logger = logging.getLogger(__name__)


class HardwareMetricReportingThread(NeptuneThread):
    def __init__(self, metric_service, metric_sending_interval_seconds):
        super(HardwareMetricReportingThread, self).__init__(is_daemon=True)
        self.__metric_service = metric_service
        self.__metric_sending_interval_seconds = metric_sending_interval_seconds

    def run(self):
        try:
            while self.should_continue_running():
                before = time.time()

                try:
                    self.__metric_service.report_and_send(timestamp=time.time())
                except (NeptuneException, HTTPError):
                    _logger.exception("Unexpected HTTP error in hardware metric reporting thread.")

                reporting_duration = time.time() - before

                time.sleep(max(0, self.__metric_sending_interval_seconds - reporting_duration))
        except Exception as e:
            _logger.debug("Unexpected error in hardware metric reporting thread: %s", e)
