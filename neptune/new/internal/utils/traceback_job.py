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
from functools import partial

from typing import Optional, List, Callable

from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.utils.uncaught_exception_handler import instance as traceback_handler
from neptune.new.run import Run

_logger = logging.getLogger(__name__)


class TracebackJob(BackgroundJob):

    def __init__(self, log_traceback_handler: Callable[[Run, List[str]], None]):
        self._uuid = uuid.uuid4()
        self._started = False
        self._log_traceback_handler = log_traceback_handler

    def start(self, run: Run):
        if not self._started:
            traceback_handler.register(self._uuid, partial(self._log_traceback_handler, run))
        self._started = True

    def stop(self):
        traceback_handler.unregister(self._uuid)

    def join(self, seconds: Optional[float] = None):
        pass
