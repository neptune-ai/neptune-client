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

import os
import sys
import threading

from typing import TextIO

from neptune.new.run import Run
from neptune.new.logging import Logger as NeptuneLogger


class StdStreamCaptureLogger:

    def __init__(self, run: Run, attribute_name: str, stream: TextIO):
        self._logger = NeptuneLogger(run, attribute_name)
        self.stream = stream
        self._thread_local = threading.local()
        self._drop_logs = False

        if sys.version_info >= (3, 7):
            try:
                # pylint: disable=no-member
                os.register_at_fork(after_in_child=self._handle_fork_in_child)
            except AttributeError:
                pass

    def _handle_fork_in_child(self):
        self._drop_logs = True

    def write(self, data: str):
        if not hasattr(self._thread_local, "inside_write"):
            self._thread_local.inside_write = False

        self.stream.write(data)
        if not self._thread_local.inside_write and not self._drop_logs:
            try:
                self._thread_local.inside_write = True
                self._logger.log(data)
            finally:
                self._thread_local.inside_write = False

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


class StdoutCaptureLogger(StdStreamCaptureLogger):

    def __init__(self, run: Run, attribute_name: str):
        super().__init__(run, attribute_name, sys.stdout)
        sys.stdout = self

    def close(self):
        sys.stdout = self.stream


class StderrCaptureLogger(StdStreamCaptureLogger):

    def __init__(self, run: Run, attribute_name: str):
        super().__init__(run, attribute_name, sys.stderr)
        sys.stderr = self

    def close(self):
        sys.stderr = self.stream
