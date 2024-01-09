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
__all__ = ["set_up_logging", "get_logger", "NEPTUNE_LOGGER_NAME"]

import logging
import sys

NEPTUNE_LOGGER_NAME = "neptune-client"
LOG_FORMAT = "%(message)s"


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


def get_logger():
    return logging.getLogger(NEPTUNE_LOGGER_NAME)


def set_up_logging():
    # setup neptune logger so that logging.getLogger(NEPTUNE_LOGGER_NAME)
    # returns configured logger
    neptune_logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    neptune_logger.propagate = False
    neptune_logger.setLevel(logging.DEBUG)

    stdout_handler = GrabbableStdoutHandler()
    stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    neptune_logger.addHandler(stdout_handler)
