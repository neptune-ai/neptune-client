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
from neptune.internal.hardware.gauges.cpu import CGroupCpuUsageGauge, SystemCpuUsageGauge
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.gauges.gpu import GpuMemoryGauge, GpuUsageGauge
from neptune.internal.hardware.gauges.memory import CGroupMemoryUsageGauge, SystemMemoryUsageGauge


class GaugeFactory(object):
    def __init__(self, gauge_mode):
        self.__gauge_mode = gauge_mode

    def create_cpu_usage_gauge(self):
        if self.__gauge_mode == GaugeMode.SYSTEM:
            return SystemCpuUsageGauge()
        elif self.__gauge_mode == GaugeMode.CGROUP:
            return CGroupCpuUsageGauge()
        else:
            raise self.__invalid_gauge_mode_exception()

    def create_memory_usage_gauge(self):
        if self.__gauge_mode == GaugeMode.SYSTEM:
            return SystemMemoryUsageGauge()
        elif self.__gauge_mode == GaugeMode.CGROUP:
            return CGroupMemoryUsageGauge()
        else:
            raise self.__invalid_gauge_mode_exception()

    @staticmethod
    def create_gpu_usage_gauge(card_index):
        return GpuUsageGauge(card_index=card_index)

    @staticmethod
    def create_gpu_memory_gauge(card_index):
        return GpuMemoryGauge(card_index=card_index)

    def __invalid_gauge_mode_exception(self):
        return ValueError(str(u'Invalid gauge mode: {}'.format(self.__gauge_mode)))
