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
__all__ = ["StdoutCaptureLogger", "StderrCaptureLogger"]

import sys
import threading
from queue import Queue
from typing import TextIO

from neptune.internal.threading.daemon import Daemon
from neptune.logging import Logger as NeptuneLogger
from neptune.metadata_containers import MetadataContainer


class StdStreamCaptureLogger:
    def __init__(self, container: MetadataContainer, attribute_name: str, stream: TextIO):
        self._logger = NeptuneLogger(container, attribute_name)
        self.stream = stream
        self._thread_local = threading.local()
        self.enabled = True
        self._log_data_queue = Queue()
        self._logging_thread = self.ReportingThread(self, "NeptuneThread_" + attribute_name)
        self._logging_thread.start()

    def pause(self):
        self._log_data_queue.put_nowait(None)
        self._logging_thread.pause()

    def resume(self):
        self._logging_thread.resume()

    def write(self, data: str):
        self.stream.write(data)
        self._log_data_queue.put_nowait(data)

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

    def close(self):
        if self.enabled:
            self._logging_thread.interrupt()
        self.enabled = False
        self._log_data_queue.put_nowait(None)
        self._logging_thread.join()

    class ReportingThread(Daemon):
        def __init__(self, logger: "StdStreamCaptureLogger", name: str):
            super().__init__(sleep_time=0, name=name)
            self._logger = logger

        @Daemon.ConnectionRetryWrapper(kill_message="Killing Neptune STD capturing thread.")
        def work(self) -> None:
            while True:
                data = self._logger._log_data_queue.get()
                if data is None:
                    break
                self._logger._logger.log(data)


class StdoutCaptureLogger(StdStreamCaptureLogger):
    def __init__(self, container: MetadataContainer, attribute_name: str):
        super().__init__(container, attribute_name, sys.stdout)
        sys.stdout = self

    def close(self):
        sys.stdout = self.stream
        super().close()


class StderrCaptureLogger(StdStreamCaptureLogger):
    def __init__(self, container: MetadataContainer, attribute_name: str):
        super().__init__(container, attribute_name, sys.stderr)
        sys.stderr = self

    def close(self, wait_for_all_logs=True):
        sys.stderr = self.stream
        super().close()
