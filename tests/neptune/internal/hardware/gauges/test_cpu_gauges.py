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
import unittest

from neptune.internal.hardware.gauges.cpu import CGroupCpuUsageGauge, SystemCpuUsageGauge
from neptune.utils import IS_WINDOWS, IS_MACOS


class TestCpuGauges(unittest.TestCase):
    @unittest.skipIf(IS_MACOS, "MacOS behaves strangely")
    def test_system_cpu_gauge(self):
        # given
        gauge = SystemCpuUsageGauge()

        # when
        cpu_usage = gauge.value()

        # then
        self.assertGreater(cpu_usage, 0.0)
        self.assertLessEqual(cpu_usage, 100.0)
        self.assertEqual(float, type(cpu_usage))

    @unittest.skipIf(IS_WINDOWS or IS_MACOS, "Windows and MacOS don't have cgroups")
    def test_cgroup_cpu_gauge_returns_zero_on_first_measurement(self):
        # given
        gauge = CGroupCpuUsageGauge()

        # when
        cpu_usage = gauge.value()

        # then
        self.assertEqual(0.0, cpu_usage)
        self.assertEqual(float, type(cpu_usage))

    @unittest.skipIf(IS_WINDOWS or IS_MACOS, "Windows and MacOS don't have cgroups")
    def test_cgroup_cpu_gauge_measurement(self):
        # given
        gauge = CGroupCpuUsageGauge()
        # and
        gauge.value()
        time.sleep(0.1)

        # when
        cpu_usage = gauge.value()

        # then
        self.assertGreater(cpu_usage, 0.0)
        self.assertLessEqual(cpu_usage, 100.0)
        self.assertEqual(float, type(cpu_usage))


if __name__ == '__main__':
    unittest.main()
