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
import time
from typing import Optional

from neptune.common.hardware.cgroup.cgroup_filesystem_reader import CGroupFilesystemReader
from neptune.common.hardware.system.system_monitor import SystemMonitor


class CGroupMonitor:
    def __init__(self, cgroup_filesystem_reader: CGroupFilesystemReader, system_monitor: SystemMonitor):
        self.__cgroup_filesystem_reader = cgroup_filesystem_reader
        self.__system_monitor = system_monitor

        self.__last_cpu_usage_measurement_timestamp_nanos: Optional[float] = None
        self.__last_cpu_cumulative_usage_nanos: Optional[float] = None

    @staticmethod
    def create() -> "CGroupMonitor":
        return CGroupMonitor(CGroupFilesystemReader(), SystemMonitor())

    def get_memory_usage_in_bytes(self) -> int:
        return self.__cgroup_filesystem_reader.get_memory_usage_in_bytes()

    def get_memory_limit_in_bytes(self) -> int:
        cgroup_mem_limit = self.__cgroup_filesystem_reader.get_memory_limit_in_bytes()
        total_virtual_memory = self.__system_monitor.virtual_memory().total
        return min(cgroup_mem_limit, total_virtual_memory)

    def get_cpu_usage_limit_in_cores(self) -> float:
        cpu_quota_micros = self.__cgroup_filesystem_reader.get_cpu_quota_micros()

        if cpu_quota_micros == -1:
            return float(self.__system_monitor.cpu_count())
        else:
            cpu_period_micros = self.__cgroup_filesystem_reader.get_cpu_period_micros()
            return float(cpu_quota_micros) / float(cpu_period_micros)

    def get_cpu_usage_percentage(self) -> float:
        current_timestamp_nanos = time.time() * 10**9
        cpu_cumulative_usage_nanos = self.__cgroup_filesystem_reader.get_cpuacct_usage_nanos()

        if self.__last_cpu_usage_measurement_timestamp_nanos is None or self.__last_cpu_cumulative_usage_nanos is None:
            current_usage = 0.0
        else:
            usage_diff = cpu_cumulative_usage_nanos - self.__last_cpu_cumulative_usage_nanos
            time_diff = current_timestamp_nanos - self.__last_cpu_usage_measurement_timestamp_nanos
            current_usage = float(usage_diff) / float(time_diff) / self.get_cpu_usage_limit_in_cores() * 100.0

        self.__last_cpu_usage_measurement_timestamp_nanos = current_timestamp_nanos
        self.__last_cpu_cumulative_usage_nanos = cpu_cumulative_usage_nanos

        # cgroup cpu usage may slightly exceed the given limit, but we don't want to show it
        return self.__clamp(current_usage, lower_limit=0.0, upper_limit=100.0)

    @staticmethod
    def __clamp(value: float, lower_limit: float, upper_limit: float) -> float:
        return max(lower_limit, min(value, upper_limit))
