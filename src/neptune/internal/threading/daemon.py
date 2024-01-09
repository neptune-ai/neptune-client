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
from enum import Enum

from neptune.common.exceptions import NeptuneConnectionLostException
from neptune.internal.utils.logger import get_logger

logger = get_logger()


class Daemon(threading.Thread):
    class DaemonState(Enum):
        INIT = 1
        WORKING = 2
        PAUSING = 3
        PAUSED = 4
        INTERRUPTED = 5
        STOPPED = 6

    def __init__(self, sleep_time: float, name):
        super().__init__(daemon=True, name=name)
        self._sleep_time = sleep_time
        self._state: Daemon.DaemonState = Daemon.DaemonState.INIT
        self._wait_condition = threading.Condition()
        self.last_backoff_time = 0  # used only with ConnectionRetryWrapper decorator

    def interrupt(self):
        with self._wait_condition:
            self._state = Daemon.DaemonState.INTERRUPTED
            self._wait_condition.notify_all()

    def pause(self):
        with self._wait_condition:
            if self._state != Daemon.DaemonState.PAUSED:
                if not self._is_interrupted():
                    self._state = Daemon.DaemonState.PAUSING
                self._wait_condition.notify_all()
                self._wait_condition.wait_for(lambda: self._state != Daemon.DaemonState.PAUSING)

    def resume(self):
        with self._wait_condition:
            if not self._is_interrupted():
                self._state = Daemon.DaemonState.WORKING
            self._wait_condition.notify_all()

    def wake_up(self):
        with self._wait_condition:
            self._wait_condition.notify_all()

    def disable_sleep(self):
        self._sleep_time = 0

    def is_running(self) -> bool:
        with self._wait_condition:
            return self._state in (
                Daemon.DaemonState.WORKING,
                Daemon.DaemonState.PAUSING,
                Daemon.DaemonState.PAUSED,
            )

    def _is_interrupted(self) -> bool:
        with self._wait_condition:
            return self._state in (Daemon.DaemonState.INTERRUPTED, Daemon.DaemonState.STOPPED)

    def run(self):
        with self._wait_condition:
            if not self._is_interrupted():
                self._state = Daemon.DaemonState.WORKING
        try:
            while not self._is_interrupted():
                with self._wait_condition:
                    if self._state == Daemon.DaemonState.PAUSING:
                        self._state = Daemon.DaemonState.PAUSED
                        self._wait_condition.notify_all()
                        self._wait_condition.wait_for(lambda: self._state != Daemon.DaemonState.PAUSED)

                if self._state == Daemon.DaemonState.WORKING:
                    self.work()
                    with self._wait_condition:
                        if self._sleep_time > 0 and self._state == Daemon.DaemonState.WORKING:
                            self._wait_condition.wait(timeout=self._sleep_time)
        finally:
            with self._wait_condition:
                self._state = Daemon.DaemonState.STOPPED
                self._wait_condition.notify_all()

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
                while not self_._is_interrupted():
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

                        with self_._wait_condition:
                            self_._wait_condition.wait(self_.last_backoff_time)
                    except Exception:
                        logger.error(
                            "Unexpected error occurred in Neptune background thread: %s",
                            self.kill_message,
                        )
                        raise

            return wrapper
