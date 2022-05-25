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

import logging
import pathlib
import sys
import typing

from neptune.new.constants import NEPTUNE_DATA_DIRECTORY

if typing.TYPE_CHECKING:
    from bravado.response import BravadoResponse
    from requests import PreparedRequest

    from neptune.new.internal.id_formats import UniqueId


LOGGER_NAME = "neptune-client"


class StdoutPassthroughHandler(logging.StreamHandler):
    """
    This class is like a StreamHandler using sys.stdout, but always uses
    whatever sys.stdout is currently set to rather than the value of
    sys.stderr at handler construction time.
    This enables neptune-client to capture stdout regardless
    of logging configuration time.
    Based on logging._StderrHandler from standard library.
    """

    def __init__(self, level=logging.NOTSET):
        # pylint: disable=non-parent-init-called,super-init-not-called
        logging.Handler.__init__(self, level)

    def filter(self, record: logging.LogRecord) -> bool:
        """skip exceptions, as they are sent to stderr via standard handler"""
        if getattr(record, "exception", False):
            return False

        return True

    @property
    def stream(self):
        return sys.stdout


def log_exceptions(exc_type, value, tb):
    logger.error("Exception %s", exc_type.__name__, exc_info=value, extra={"exception": True})
    sys.__excepthook__(exc_type, value, tb)


def trace_request(unique_id: "UniqueId", response: "BravadoResponse"):
    logger = get_logger_for_metadata_container(unique_id)
    try:
        request: "PreparedRequest" = response.incoming_response._delegate.request
        logger.debug(
            "request: %s status: %s elapsed: %.3f",
            request.url,
            response.metadata.status_code,
            response.metadata.elapsed_time,
        )
    except Exception:
        logger.debug("failed to trace request for %s", response)


def get_logger_for_metadata_container(unique_id: "UniqueId") -> logging.Logger:
    return logging.getLogger(f"{LOGGER_NAME}.{unique_id}")


def build_handler_for_metadata_container(unique_id: "UniqueId") -> logging.Handler:
    logs_dir = pathlib.Path(NEPTUNE_DATA_DIRECTORY) / "logs"
    logs_dir.mkdir(exist_ok=True, parents=True)
    handler = logging.FileHandler(logs_dir / unique_id)
    handler.setFormatter(
        logging.Formatter("%(name)s|%(asctime)s|%(levelname)s|%(pathname)s:%(lineno)d|%(message)s")
    )
    return handler


logger = logging.getLogger(LOGGER_NAME)

logger.propagate = False
logger.setLevel(level=logging.DEBUG)
stdout_handler = StdoutPassthroughHandler(level=logging.INFO)
stdout_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(stdout_handler)

# log exceptions
sys.excepthook = log_exceptions
