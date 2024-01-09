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
__all__ = ["GPUMonitor"]

from neptune.internal.utils.logger import get_logger
from neptune.vendor.pynvml import (
    NVMLError,
    nvmlDeviceGetCount,
    nvmlDeviceGetHandleByIndex,
    nvmlDeviceGetMemoryInfo,
    nvmlDeviceGetUtilizationRates,
    nvmlInit,
)

_logger = get_logger()


class GPUMonitor(object):

    nvml_error_printed = False

    def get_card_count(self):
        return self.__nvml_get_or_else(nvmlDeviceGetCount, default=0)

    def get_card_usage_percent(self, card_index):
        return self.__nvml_get_or_else(
            lambda: float(nvmlDeviceGetUtilizationRates(nvmlDeviceGetHandleByIndex(card_index)).gpu)
        )

    def get_card_used_memory_in_bytes(self, card_index):
        return self.__nvml_get_or_else(lambda: nvmlDeviceGetMemoryInfo(nvmlDeviceGetHandleByIndex(card_index)).used)

    def get_top_card_memory_in_bytes(self):
        def read_top_card_memory_in_bytes():
            return self.__nvml_get_or_else(
                lambda: [
                    nvmlDeviceGetMemoryInfo(nvmlDeviceGetHandleByIndex(card_index)).total
                    for card_index in range(nvmlDeviceGetCount())
                ],
                default=0,
            )

        memory_per_card = read_top_card_memory_in_bytes()
        if not memory_per_card:
            return 0
        return max(memory_per_card)

    def __nvml_get_or_else(self, getter, default=None):
        try:
            nvmlInit()
            return getter()
        except NVMLError as e:
            if not GPUMonitor.nvml_error_printed:
                warning = (
                    "Info (NVML): %s. GPU usage metrics may not be reported. For more information, "
                    "see https://docs.neptune.ai/help/nvml_error/"
                )
                _logger.warning(warning, e)
                GPUMonitor.nvml_error_printed = True
            return default
