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
import unittest

import psutil

from neptune.internal.hardware.gauges.memory import CGroupMemoryUsageGauge, SystemMemoryUsageGauge
from neptune.utils import IS_WINDOWS, IS_MACOS


class TestMemoryGauges(unittest.TestCase):
    def setUp(self):
        self.system_memory_gb = psutil.virtual_memory().total / float(2 ** 30)

    def test_system_memory_gauge(self):
        # given
        gauge = SystemMemoryUsageGauge()

        # when
        memory_gb = gauge.value()

        # then
        self.assertGreater(memory_gb, 0)
        self.assertLess(memory_gb, self.system_memory_gb)
        self.assertEqual(float, type(memory_gb))

    @unittest.skipIf(IS_WINDOWS or IS_MACOS, "Windows and MacOS don't have cgroups")
    def test_cgroup_memory_gauge(self):
        # given
        gauge = CGroupMemoryUsageGauge()

        # when
        memory_gb = gauge.value()

        # then
        self.assertGreater(memory_gb, 0)
        self.assertLess(memory_gb, self.system_memory_gb)
        self.assertEqual(float, type(memory_gb))


if __name__ == '__main__':
    unittest.main()
