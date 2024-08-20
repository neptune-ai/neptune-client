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
from neptune.common.hardware.constants import (
    BYTES_IN_ONE_GB,
    MILLIWATTS_IN_ONE_WATT,
)
from neptune.common.hardware.gauges.gauge import Gauge
from neptune.common.hardware.gpu.gpu_monitor import GPUMonitor


class GpuUsageGauge(Gauge):
    def __init__(self, card_index):
        self.card_index = card_index
        self.__gpu_monitor = GPUMonitor()

    def name(self):
        return str(self.card_index)

    def value(self):
        return self.__gpu_monitor.get_card_usage_percent(self.card_index)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.card_index == other.card_index

    def __repr__(self):
        return "GpuUsageGauge"


class GpuMemoryGauge(Gauge):
    def __init__(self, card_index):
        self.card_index = card_index
        self.__gpu_monitor = GPUMonitor()

    def name(self):
        return str(self.card_index)

    def value(self):
        return self.__gpu_monitor.get_card_used_memory_in_bytes(self.card_index) / float(BYTES_IN_ONE_GB)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.card_index == other.card_index

    def __repr__(self):
        return "GpuMemoryGauge"


class GpuPowerGauge(Gauge):
    def __init__(self, card_index):
        self.card_index = card_index
        self.__gpu_monitor = GPUMonitor()

    def name(self):
        return str(self.card_index)

    def value(self):
        power_usage = self.__gpu_monitor.get_card_power_usage(self.card_index)
        return None if power_usage is None else power_usage / MILLIWATTS_IN_ONE_WATT

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.card_index == other.card_index

    def __repr__(self):
        return "GpuPowerGauge"
