#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from neptune.new import Run
from neptune.new.attributes.constants import MONITORING_DEFAULT_LOG_HANDLER_ATTRIBUTE_PATH
from neptune.new.internal.utils import verify_type
from neptune.new.logging import Logger


class NeptuneLogHandler(logging.Handler):
    """Custom handler that sends logged messages to Neptune

    Args:
        run (Run): An existing run reference (as returned by `neptune.init`)
            Logger will send messages as a `StringSeries` attribute on this run.
        level (int, optional): Log level of the handler. Defaults to `logging.NOTSET`,
            which logs everything that matches logger's level.
        path (str, optional): Path to the `StringSeries` attribute used for logging. Default to `None`.
            If `None`, `'monitoring/python_logger'` is used.

    Usage example:
        >>> import logging
        >>> import neptune.new as neptune
        >>> from neptune.new.logging.handler import NeptuneLogHandler

        >>> logger = logging.getLogger("root_experiment")
        >>> logger.setLevel(logging.DEBUG)

        >>> run = neptune.init(project=‘neptune/sandbox’)
        >>> npt_handler = NeptuneLoggerHandler(run=run, level=logging.WARNING, path="logging/root_experiment")
        >>> logger.addHandler(npt_handler)

        >>> logger.debug("this won't be sent")
        >>> logger.error("but this will!!%d", 1)
    """
    def __init__(self, *, run: Run, level=logging.NOTSET, path: str = None):
        verify_type("run", run, Run)
        verify_type("level", level, int)
        if path is None:
            path = MONITORING_DEFAULT_LOG_HANDLER_ATTRIBUTE_PATH
        verify_type("path", path, str)

        super().__init__(level=level)
        self._logger = Logger(run, path)

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        self._logger.log(message)
