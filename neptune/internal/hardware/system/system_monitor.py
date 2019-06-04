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


class SystemMonitor(object):
    @staticmethod
    def requirements_installed():
        # pylint:disable=unused-import,unused-variable
        try:
            import psutil
            return True
        except ImportError:
            _logger.warning('psutil is not installed. Hardware metrics will not be collected.')
            return False

    def cpu_count(self):
        return self._psutil().cpu_count()

    def cpu_percent(self):
        return self._psutil().cpu_percent()

    def virtual_memory(self):
        return self._psutil().virtual_memory()

    @classmethod
    def _psutil(cls):
        import psutil
        return psutil
