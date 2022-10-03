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

import sys
import threading
from typing import TextIO

from neptune.new.logging import Logger as NeptuneLogger
from neptune.new.metadata_containers import MetadataContainer


class StdStreamCaptureLogger:
    def __init__(self, container: MetadataContainer, attribute_name: str, stream: TextIO):
        self._logger = NeptuneLogger(container, attribute_name)
        self.stream = stream
        self._thread_local = threading.local()
        self.enabled = True

    def write(self, data: str):
        if not hasattr(self._thread_local, "inside_write"):
            self._thread_local.inside_write = False

        self.stream.write(data)
        if self.enabled and not self._thread_local.inside_write:
            try:
                self._thread_local.inside_write = True
                self._logger.log(data)
            finally:
                self._thread_local.inside_write = False

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

    def close(self):
        self.enabled = False


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

    def close(self):
        sys.stderr = self.stream
        super().close()
