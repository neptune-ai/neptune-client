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
import threading


class NeptuneThread(threading.Thread):
    def __init__(self, is_daemon):
        super(NeptuneThread, self).__init__(target=self.run)
        self.setDaemon(is_daemon)
        self._interrupted = threading.Event()

    def is_interrupted(self):
        return self._interrupted.is_set()

    def interrupt(self):
        self._interrupted.set()

    def run(self):
        raise NotImplementedError()
