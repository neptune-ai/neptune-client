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

from bravado.exception import HTTPUnprocessableEntity

from neptune.legacy.internal.threads.neptune_thread import NeptuneThread

_logger = logging.getLogger(__name__)


class PingThread(NeptuneThread):
    PING_INTERVAL_SECS = 5

    def __init__(self, backend, experiment):
        super(PingThread, self).__init__(is_daemon=True)

        self.__backend = backend
        self.__experiment = experiment

    def run(self):
        while self.should_continue_running():
            try:
                self.__backend.ping_experiment(self.__experiment)
            except HTTPUnprocessableEntity:
                # A 422 error means that we tried to ping the job after marking it as completed.
                # In this case, this thread is not needed anymore.
                break
            except Exception:
                _logger.exception("Unexpected error in ping thread.")
            self._interrupted.wait(self.PING_INTERVAL_SECS)
