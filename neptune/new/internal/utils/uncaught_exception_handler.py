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
import sys
import threading
import traceback
import uuid
from typing import TYPE_CHECKING, Callable, List
from platform import node as get_hostname

if TYPE_CHECKING:
    pass

_logger = logging.getLogger(__name__)


class UncaughtExceptionHandler:

    def __init__(self):
        self._previous_uncaught_exception_handler = None
        self._handlers = dict()
        self._lock = threading.Lock()

    def activate(self):
        with self._lock:
            this = self
            def exception_handler(exc_type, exc_val, exc_tb):
                header_lines = [
                    f"An uncaught exception occurred while run was active on worker {get_hostname()}.",
                    "Making run as failed",
                    "Traceback:"
                ]

                traceback_lines = header_lines + traceback.format_tb(exc_tb) + [repr(exc_val)]
                for _, handler in self._handlers.items():
                    handler(traceback_lines)

                # pylint: disable=protected-access
                this._previous_uncaught_exception_handler(exc_type, exc_val, exc_tb)

            if self._previous_uncaught_exception_handler is None:
                self._previous_uncaught_exception_handler = sys.excepthook
                sys.excepthook = exception_handler

    def deactivate(self):
        with self._lock:
            sys.excepthook = self._previous_uncaught_exception_handler
            self._previous_uncaught_exception_handler = None

    def register(self, uid: uuid.UUID, handler: Callable[[List[str]], None]):
        with self._lock:
            self._handlers[uid] = handler

    def unregister(self, uid: uuid.UUID):
        with self._lock:
            if uid in self._handlers:
                del self._handlers[uid]


instance = UncaughtExceptionHandler()
