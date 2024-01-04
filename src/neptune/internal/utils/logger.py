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
__all__ = ["logger"]

import logging
import sys

LOGGER_NAME = "neptune-client"


class GrabbableStdoutHandler(logging.StreamHandler):
    """
    This class is like a StreamHandler using sys.stdout, but always uses
    whatever sys.stdout is currently set to rather than the value of
    sys.stderr at handler construction time.
    This enables neptune-client to capture stdout regardless
    of logging configuration time.
    Based on logging._StderrHandler from standard library.
    """

    def __init__(self, level=logging.NOTSET):
        logging.Handler.__init__(self, level)

    @property
    def stream(self):
        return sys.stdout


class CommonPrefixLogger(logging.Logger):
    def __init__(self, name: str) -> None:
        logging.Logger.__init__(self, f"{LOGGER_NAME}:{name}")
        self.propagate = False
        self.setLevel(level=logging.DEBUG)
        stdout_handler = GrabbableStdoutHandler()
        stdout_handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
        self.addHandler(stdout_handler)


logging.setLoggerClass(CommonPrefixLogger)


logger = logging.getLogger(LOGGER_NAME)
