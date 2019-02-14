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

from __future__ import print_function

import logging

from bravado.exception import HTTPUnprocessableEntity

from neptune.internal.threads.neptune_thread import NeptuneThread


class PingThread(NeptuneThread):
    PING_INTERVAL_SECS = 5

    def __init__(self, client, experiment_id):
        super(PingThread, self).__init__(is_daemon=True)
        self.__logger = logging.getLogger(__name__)

        self.__client = client
        self.__experiment_id = experiment_id

    def run(self):
        while not self.is_interrupted():
            try:
                self.__client.ping_experiment(self.__experiment_id)
            except HTTPUnprocessableEntity:
                # A 422 error means that we tried to ping the job after marking it as completed.
                # In this case, this thread is not needed anymore.
                break
            except Exception as e:
                self.__logger.error(e)
            self._interrupted.wait(self.PING_INTERVAL_SECS)
