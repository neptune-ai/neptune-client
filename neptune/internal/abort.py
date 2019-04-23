#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

_logger = logging.getLogger(__name__)


class CustomAbortImpl(object):
    def __init__(self, runnable):
        self.__runnable = runnable

    def abort(self):
        self.__runnable()


class DefaultAbortImpl(object):
    KILL_TIMEOUT = 5

    def __init__(self, pid):
        self._pid = pid

    @staticmethod
    def requirements_installed():
        # pylint:disable=unused-import,unused-variable
        try:
            import psutil
            return True
        except ImportError:
            _logger.warning('psutil is not installed. The experiment will not be abortable.')
            return False

    def abort(self):
        import psutil

        process = None
        try:
            process = psutil.Process(self._pid)
        except psutil.NoSuchProcess:
            pass

        if process is not None:
            processes = self._get_processes(process)
            for p in processes:
                self._abort(p)
            _, alive = psutil.wait_procs(processes, timeout=self.KILL_TIMEOUT)
            for p in alive:
                self._kill(p)

    @staticmethod
    def _get_processes(process):
        import psutil

        try:
            return [process] + process.children(recursive=True)
        except psutil.NoSuchProcess:
            return []

    def _abort(self, process):
        import psutil

        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass

    def _kill(self, process):
        import psutil

        for process in self._get_processes(process):
            try:
                if process.is_running():
                    process.kill()
            except psutil.NoSuchProcess:
                pass
