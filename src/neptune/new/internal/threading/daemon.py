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
__all__ = ["Daemon"]

import abc
import functools
import threading
import time

from neptune.common.exceptions import NeptuneConnectionLostException
from neptune.new.internal.utils.logger import logger


class Daemon(threading.Thread):
    def __init__(self, sleep_time: float, name):
        super().__init__(daemon=True, name=name)
        self._sleep_time = sleep_time
        self._interrupted = False
        self._event = threading.Event()
        self._is_running = False
        self.last_backoff_time = 0  # used only with ConnectionRetryWrapper decorator

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

    class ConnectionRetryWrapper:
        INITIAL_RETRY_BACKOFF = 2
        MAX_RETRY_BACKOFF = 120

        def __init__(self, kill_message):
            self.kill_message = kill_message

        def __call__(self, func):
            @functools.wraps(func)
            def wrapper(self_: Daemon, *args, **kwargs):
                while not self_._interrupted:
                    try:
                        result = func(self_, *args, **kwargs)
                        if self_.last_backoff_time > 0:
                            self_.last_backoff_time = 0
                            logger.info("Communication with Neptune restored!")
                        return result
                    except NeptuneConnectionLostException as e:
                        if self_.last_backoff_time == 0:
                            logger.warning(
                                "Experiencing connection interruptions."
                                " Will try to reestablish communication with Neptune."
                                " Internal exception was: %s",
                                e.cause.__class__.__name__,
                            )
                            self_.last_backoff_time = self.INITIAL_RETRY_BACKOFF
                        else:
                            self_.last_backoff_time = min(self_.last_backoff_time * 2, self.MAX_RETRY_BACKOFF)
                        time.sleep(self_.last_backoff_time)
                    except Exception:
                        logger.error(
                            "Unexpected error occurred in Neptune background thread: %s",
                            self.kill_message,
                        )
                        raise

            return wrapper
