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
__all__ = ["StdoutCaptureBackgroundJob", "StderrCaptureBackgroundJob"]

from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune.internal.background_job import BackgroundJob
from neptune.internal.streams.std_stream_capture_logger import (
    StderrCaptureLogger,
    StdoutCaptureLogger,
)

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer


class StdoutCaptureBackgroundJob(BackgroundJob):
    def __init__(self, attribute_name: str):
        self._attribute_name = attribute_name
        self._logger = None

    def start(self, container: "MetadataContainer"):
        self._logger = StdoutCaptureLogger(container, self._attribute_name)

    def stop(self):
        self._logger.close()

    def pause(self):
        self._logger.pause()

    def resume(self):
        self._logger.resume()

    def join(self, seconds: Optional[float] = None):
        pass


class StderrCaptureBackgroundJob(BackgroundJob):
    def __init__(self, attribute_name: str):
        self._attribute_name = attribute_name
        self._logger = None

    def start(self, container: "MetadataContainer"):
        self._logger = StderrCaptureLogger(container, self._attribute_name)

    def stop(self):
        self._logger.close()

    def pause(self):
        self._logger.pause()

    def resume(self):
        self._logger.resume()

    def join(self, seconds: Optional[float] = None):
        pass
