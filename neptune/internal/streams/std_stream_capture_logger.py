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

import sys
import threading

from typing import TextIO

from neptune.experiment import Experiment
from neptune.logging import Logger as NeptuneLogger


class StdStreamCaptureLogger:

    def __init__(self, experiment: Experiment, attribute_name: str, stream: TextIO):
        self._logger = NeptuneLogger(experiment, attribute_name)
        self.stream = stream
        self._thread_local = threading.local()
        self._thread_local.inside_write = False

    def write(self, data: str):
        try:
            self.stream.write(data)
            if not self._thread_local.inside_write:
                self._thread_local.inside_write = True
                self._logger.log(data)
        finally:
            self._thread_local.inside_write = False

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


class StdoutCaptureLogger(StdStreamCaptureLogger):

    def __init__(self, experiment: Experiment, attribute_name: str):
        super().__init__(experiment, attribute_name, sys.stdout)
        sys.stdout = self

    def close(self):
        sys.stdout = self.stream


class StderrCaptureLogger(StdStreamCaptureLogger):

    def __init__(self, experiment: Experiment, attribute_name: str):
        super().__init__(experiment, attribute_name, sys.stderr)
        sys.stdout = self

    def close(self):
        sys.stderr = self.stream
