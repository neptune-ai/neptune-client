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
from mock import MagicMock

from neptune.internal.hardware.gauges.cpu import SystemCpuUsageGauge
from neptune.internal.hardware.gauges.gauge_factory import GaugeFactory
from neptune.internal.hardware.gauges.gpu import GpuMemoryGauge, GpuUsageGauge
from neptune.internal.hardware.gauges.memory import SystemMemoryUsageGauge


class GaugesFixture(object):
    def __init__(self):
        self.gauge_factory = MagicMock(spec_set=GaugeFactory)

        self.cpu_gauge_value = 1.0
        self.memory_gauge_value = 2.0

        self.gpu0_usage_gauge_value = 3.0
        self.gpu1_usage_gauge_value = 4.0

        self.gpu0_memory_gauge_value = 5.0
        self.gpu1_memory_gauge_value = 6.0

        cpu_gauge = MagicMock(wraps=SystemCpuUsageGauge())
        cpu_gauge.value.return_value = self.cpu_gauge_value
        self.gauge_factory.create_cpu_usage_gauge.return_value = cpu_gauge

        ram_gauge = MagicMock(wraps=SystemMemoryUsageGauge())
        ram_gauge.value.return_value = self.memory_gauge_value
        self.gauge_factory.create_memory_usage_gauge.return_value = ram_gauge

        gpu_usage_gauge_0 = MagicMock(wraps=GpuUsageGauge(card_index=0))
        gpu_usage_gauge_0.value.return_value = self.gpu0_usage_gauge_value

        gpu_usage_gauge_2 = MagicMock(wraps=GpuUsageGauge(card_index=2))
        gpu_usage_gauge_2.value.return_value = self.gpu1_usage_gauge_value

        self.gauge_factory.create_gpu_usage_gauge.side_effect = \
            lambda card_index: gpu_usage_gauge_0 if card_index == 0 else gpu_usage_gauge_2

        gpu_memory_gauge_0 = MagicMock(wraps=GpuMemoryGauge(card_index=0))
        gpu_memory_gauge_0.value.return_value = self.gpu0_memory_gauge_value

        gpu_memory_gauge_2 = MagicMock(wraps=GpuMemoryGauge(card_index=2))
        gpu_memory_gauge_2.value.return_value = self.gpu1_memory_gauge_value
        self.gauge_factory.create_gpu_memory_gauge.side_effect = \
            lambda card_index: gpu_memory_gauge_0 if card_index == 0 else gpu_memory_gauge_2
