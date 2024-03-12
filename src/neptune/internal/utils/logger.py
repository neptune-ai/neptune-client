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
__all__ = ["get_logger", "get_disabled_logger", "NEPTUNE_LOGGER_NAME"]

import logging
import sys

NEPTUNE_LOGGER_NAME = "neptune"
NEPTUNE_NO_PREFIX_LOGGER_NAME = "neptune_no_prefix"
NEPTUNE_NOOP_LOGGER_NAME = "neptune_noop"
LOG_FORMAT = "[%(name)s] [%(levelname)s] %(message)s"
NO_PREFIX_FORMAT = "%(message)s"


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.levelname = record.levelname.lower().ljust(len("warning"))
        formatter = logging.Formatter(LOG_FORMAT)
        return formatter.format(record)


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


def get_logger(with_prefix: bool = True) -> logging.Logger:
    name = NEPTUNE_LOGGER_NAME if with_prefix else NEPTUNE_NO_PREFIX_LOGGER_NAME

    return logging.getLogger(name)


def get_disabled_logger() -> logging.Logger:
    return logging.getLogger(NEPTUNE_NOOP_LOGGER_NAME)


def _set_up_logging():
    # setup neptune logger so that logging.getLogger(NEPTUNE_LOGGER_NAME)
    # returns configured logger
    neptune_logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    neptune_logger.propagate = False

    stdout_handler = GrabbableStdoutHandler()
    stdout_handler.setFormatter(CustomFormatter())
    neptune_logger.addHandler(stdout_handler)

    neptune_logger.setLevel(logging.INFO)


def _set_up_no_prefix_logging():
    neptune_logger = logging.getLogger(NEPTUNE_NO_PREFIX_LOGGER_NAME)
    neptune_logger.propagate = False

    stdout_handler = GrabbableStdoutHandler()
    stdout_handler.setFormatter(logging.Formatter(NO_PREFIX_FORMAT))
    neptune_logger.addHandler(stdout_handler)

    neptune_logger.setLevel(logging.INFO)


def _set_up_disabled_logging():
    neptune_logger = logging.getLogger(NEPTUNE_NOOP_LOGGER_NAME)

    neptune_logger.setLevel(logging.CRITICAL)


_set_up_logging()
_set_up_no_prefix_logging()
_set_up_disabled_logging()
