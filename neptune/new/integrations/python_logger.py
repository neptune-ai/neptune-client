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
import threading

from neptune.new import Run
from neptune.new.internal.utils import verify_type
from neptune.new.logging import Logger
from neptune.new.run import RunState
from neptune.new.version import version as neptune_client_version


INTEGRATION_VERSION_KEY = 'source_code/integrations/neptune-python-logger'


class NeptuneHandler(logging.Handler):
    """Handler that sends the log records created by the logger to Neptune

    Args:
        run (Run): An existing run reference (as returned by `neptune.init`)
            Logger will send messages as a `StringSeries` field on this run.
        level (int, optional): Log level of the handler. Defaults to `logging.NOTSET`,
            which logs everything that matches logger's level.
        path (str, optional): Path to the `StringSeries` field used for logging. Default to `None`.
            If `None`, `'monitoring/python_logger'` is used.

    Examples:
        >>> import logging
        >>> import neptune.new as neptune
        >>> from neptune.new.integrations.python_logger import NeptuneHandler

        >>> logger = logging.getLogger("root_experiment")
        >>> logger.setLevel(logging.DEBUG)

        >>> run = neptune.init(project="neptune/sandbox")
        >>> npt_handler = NeptuneHandler(run=run)
        >>> logger.addHandler(npt_handler)

        >>> logger.debug("Starting data preparation")
        ...
        >>> logger.debug("Data preparation done")
    """
    def __init__(self, *, run: Run, level=logging.NOTSET, path: str = None):
        verify_type("run", run, Run)
        verify_type("level", level, int)
        if path is None:
            path = f"{run.monitoring_namespace}/python_logger"
        verify_type("path", path, str)

        super().__init__(level=level)
        self._run = run
        self._logger = Logger(run, path)
        self._thread_local = threading.local()

        self._run[INTEGRATION_VERSION_KEY] = neptune_client_version

    def emit(self, record: logging.LogRecord) -> None:
        if not hasattr(self._thread_local, "inside_write"):
            self._thread_local.inside_write = False

        if self._run._state == RunState.STARTED and not self._thread_local.inside_write:  # pylint: disable=protected-access
            try:
                self._thread_local.inside_write = True
                message = self.format(record)
                self._logger.log(message)
            finally:
                self._thread_local.inside_write = False
