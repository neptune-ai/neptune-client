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

from neptune.internal.hardware.cgroup.cgroup_monitor import CGroupMonitor
from neptune.internal.hardware.gauges.gauge import Gauge
from neptune.internal.hardware.system.system_monitor import SystemMonitor


class SystemCpuUsageGauge(Gauge):
    def __init__(self):
        self.__system_monitor = SystemMonitor()

    def name(self):
        return "cpu"

    def value(self):
        return self.__system_monitor.cpu_percent()

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __repr__(self):
        return str("SystemCpuUsageGauge")


class CGroupCpuUsageGauge(Gauge):
    def __init__(self):
        self.__cgroup_monitor = CGroupMonitor.create()

    def name(self):
        return "cpu"

    def value(self):
        return self.__cgroup_monitor.get_cpu_usage_percentage()

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __repr__(self):
        return str("CGroupCpuUsageGauge")
