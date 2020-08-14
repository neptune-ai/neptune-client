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
import time

from py3nvml.py3nvml import NVMLError, nvmlDeviceGetCount, nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo, \
    nvmlDeviceGetUtilizationRates, nvmlInit

_logger = logging.getLogger(__name__)

class GPUMonitor(object):

    nvml_error_time = 0
    nvml_error_period = 30

    def get_card_count(self):
        return self.__nvml_get_or_else(nvmlDeviceGetCount, default=0)

    def get_card_usage_percent(self, card_index):
        # pylint: disable=no-member
        # pylint incorrectly detects that function nvmlDeviceGetUtilizationRates returns str
        return self.__nvml_get_or_else(
            lambda: float(nvmlDeviceGetUtilizationRates(nvmlDeviceGetHandleByIndex(card_index)).gpu))

    def get_card_used_memory_in_bytes(self, card_index):
        # pylint: disable=no-member
        # pylint incorrectly detects that function nvmlDeviceGetMemoryInfo returns str
        return self.__nvml_get_or_else(lambda: nvmlDeviceGetMemoryInfo(nvmlDeviceGetHandleByIndex(card_index)).used)

    def get_top_card_memory_in_bytes(self):
        def read_top_card_memory_in_bytes():
            # pylint: disable=no-member
            # pylint incorrectly detects that function nvmlDeviceGetMemoryInfo returns str
            return self.__nvml_get_or_else(lambda: [
                nvmlDeviceGetMemoryInfo(nvmlDeviceGetHandleByIndex(card_index)).total
                for card_index in range(nvmlDeviceGetCount())
            ], default=0)

        memory_per_card = read_top_card_memory_in_bytes()
        if not memory_per_card:
            return 0
        return max(memory_per_card)

    def __nvml_get_or_else(self, getter, default=None):
        try:
            nvmlInit()
            return getter()
        except NVMLError as e:
            timestamp = time.time()
            if timestamp - GPUMonitor.nvml_error_time > GPUMonitor.nvml_error_period:
                _logger.warning("NVMLError: %s - GPU usage metrics may not be reported.", e)
                GPUMonitor.nvml_error_time = timestamp
            return default
