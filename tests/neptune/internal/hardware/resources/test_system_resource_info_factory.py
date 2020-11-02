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

import os
import unittest

from mock import MagicMock

from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.gpu.gpu_monitor import GPUMonitor
from neptune.internal.hardware.resources.system_resource_info_factory import SystemResourceInfoFactory
from neptune.internal.hardware.system.system_monitor import SystemMonitor
from neptune.utils import IS_WINDOWS, IS_MACOS
from tests.neptune.utils.assertions import AssertionExtensions


class TestSystemResourceInfoFactoryIntegration(unittest.TestCase, AssertionExtensions):
    def test_whole_system_resource_info(self):
        # given
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=GPUMonitor(), os_environ=os.environ)

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assert_float_greater_than(resource_info.cpu_core_count, 0)
        self.assert_int_greater_than(resource_info.memory_amount_bytes, 0)
        self.assert_int_greater_or_equal(resource_info.gpu_card_count, 0)
        self.assert_int_greater_or_equal(resource_info.gpu_memory_amount_bytes, 0)

    @unittest.skipIf(IS_WINDOWS or IS_MACOS, "Windows and MacOS don't have cgroups")
    def test_cgroup_resource_info(self):
        # given
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=GPUMonitor(), os_environ=os.environ)

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.CGROUP)

        # then
        self.assert_float_greater_than(resource_info.cpu_core_count, 0)
        self.assert_int_greater_than(resource_info.memory_amount_bytes, 0)
        self.assert_int_greater_or_equal(resource_info.gpu_card_count, 0)
        self.assert_int_greater_or_equal(resource_info.gpu_memory_amount_bytes, 0)


class TestSystemResourceInfoFactory(unittest.TestCase):
    def test_gpu_card_indices_without_cuda_env_variable(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 2
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ=dict())

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([0, 1], resource_info.gpu_card_indices)

    def test_gpu_card_indices_based_on_cuda_env_variable(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 4
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ={u'CUDA_VISIBLE_DEVICES': u'1,3'})

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([1, 3], resource_info.gpu_card_indices)

    def test_empty_gpu_card_indices_on_cuda_env_variable_minus_one(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 4
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ={u'CUDA_VISIBLE_DEVICES': u'-1'})

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([], resource_info.gpu_card_indices)

    def test_should_ignore_gpu_indices_after_index_out_of_range(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 4
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ={u'CUDA_VISIBLE_DEVICES': u'1,3,5,2'})

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([1, 3], resource_info.gpu_card_indices)

    def test_ignore_empty_cuda_env_variable(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 2
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ={u'CUDA_VISIBLE_DEVICES': u''})

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([0, 1], resource_info.gpu_card_indices)

    def test_should_ignore_invalid_cuda_env_variable_syntax(self):
        # given
        gpu_monitor = MagicMock(spec_set=GPUMonitor)
        gpu_monitor.get_card_count.return_value = 4
        # and
        system_resource_info_factory = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=gpu_monitor, os_environ={u'CUDA_VISIBLE_DEVICES': u'1,3,abc'})

        # when
        resource_info = system_resource_info_factory.create(GaugeMode.SYSTEM)

        # then
        self.assertEqual([0, 1, 2, 3], resource_info.gpu_card_indices)


if __name__ == '__main__':
    unittest.main()
