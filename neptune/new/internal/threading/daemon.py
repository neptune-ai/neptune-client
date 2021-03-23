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
import abc
import threading


class Daemon(threading.Thread):

    def __init__(self, sleep_time: float):
        super().__init__(daemon=True)
        self._sleep_time = sleep_time
        self._interrupted = False
        self._event = threading.Event()
        self._is_running = False

    def interrupt(self):
        self._interrupted = True
        self.wake_up()

    def wake_up(self):
        self._event.set()

    def disable_sleep(self):
        self._sleep_time = 0

    def is_running(self) -> bool:
        return self._is_running

    def run(self):
        self._is_running = True
        try:
            while not self._interrupted:
                self.work()
                if self._sleep_time > 0 and not self._interrupted:
                    self._event.wait(timeout=self._sleep_time)
                    self._event.clear()
        finally:
            self._is_running = False

    @abc.abstractmethod
    def work(self):
        pass
