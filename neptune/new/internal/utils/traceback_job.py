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

import logging
import uuid

from typing import TYPE_CHECKING, Optional, List

from neptune.new.attributes.constants import SYSTEM_FAILED_ATTRIBUTE_PATH
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.utils.uncaught_exception_handler import instance as traceback_handler

if TYPE_CHECKING:
    from neptune.new.run import Run

_logger = logging.getLogger(__name__)


class TracebackJob(BackgroundJob):

    def __init__(self, path: str, fail_on_exception: bool = True):
        self._uuid = uuid.uuid4()
        self._started = False
        self._path = path
        self._fail_on_exception = fail_on_exception

    def start(self, run: 'Run'):
        if not self._started:
            path = self._path
            fail_on_exception = self._fail_on_exception

            def log_traceback(stacktrace_lines: List[str]):
                run[path].log(stacktrace_lines)
                if fail_on_exception:
                    run[SYSTEM_FAILED_ATTRIBUTE_PATH] = True

            traceback_handler.register(self._uuid, log_traceback)
        self._started = True

    def stop(self):
        traceback_handler.unregister(self._uuid)

    def join(self, seconds: Optional[float] = None):
        pass
