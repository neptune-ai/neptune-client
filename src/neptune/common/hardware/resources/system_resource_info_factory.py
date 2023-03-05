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
from typing import (
    TYPE_CHECKING,
    Dict,
)

from neptune.common.hardware.cgroup.cgroup_monitor import CGroupMonitor
from neptune.common.hardware.gauges.gauge_mode import GaugeMode
from neptune.common.hardware.resources.gpu_card_indices_provider import GPUCardIndicesProvider
from neptune.common.hardware.resources.system_resource_info import SystemResourceInfo

if TYPE_CHECKING:
    from neptune.common.hardware.gpu.gpu_monitor import GPUMonitor
    from neptune.common.hardware.system.system_monitor import SystemMonitor


class SystemResourceInfoFactory:
    def __init__(self, system_monitor: "SystemMonitor", gpu_monitor: "GPUMonitor", os_environ: Dict[str, str]):
        self.__system_monitor = system_monitor
        self.__gpu_monitor = gpu_monitor
        self.__gpu_card_indices_provider = GPUCardIndicesProvider(
            cuda_visible_devices=os_environ.get("CUDA_VISIBLE_DEVICES"),
            gpu_card_count=self.__gpu_monitor.get_card_count(),
        )

    def create(self, gauge_mode: str) -> SystemResourceInfo:
        if gauge_mode == GaugeMode.SYSTEM:
            return self.__create_whole_system_resource_info()
        elif gauge_mode == GaugeMode.CGROUP:
            return self.__create_cgroup_resource_info()
        else:
            raise ValueError(str("Unknown gauge mode: {}".format(gauge_mode)))

    def __create_whole_system_resource_info(self) -> SystemResourceInfo:
        return SystemResourceInfo(
            cpu_core_count=float(self.__system_monitor.cpu_count()),
            memory_amount_bytes=self.__system_monitor.virtual_memory().total,
            gpu_card_indices=self.__gpu_card_indices_provider.get(),
            gpu_memory_amount_bytes=self.__gpu_monitor.get_top_card_memory_in_bytes(),
        )

    def __create_cgroup_resource_info(self) -> SystemResourceInfo:
        cgroup_monitor = CGroupMonitor.create()

        return SystemResourceInfo(
            cpu_core_count=cgroup_monitor.get_cpu_usage_limit_in_cores(),
            memory_amount_bytes=cgroup_monitor.get_memory_limit_in_bytes(),
            gpu_card_indices=self.__gpu_card_indices_provider.get(),
            gpu_memory_amount_bytes=self.__gpu_monitor.get_top_card_memory_in_bytes(),
        )
