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

from mock import MagicMock, patch

from neptune.internal.hardware.constants import BYTES_IN_ONE_GB
from neptune.internal.hardware.gauges.gpu import GpuMemoryGauge, GpuUsageGauge


@patch('neptune.internal.hardware.gpu.gpu_monitor.nvmlInit', MagicMock())
class TestGPUGauges(unittest.TestCase):
    def setUp(self):
        self.card_index = 2
        self.gpu_card_handle = MagicMock()

        patcher = patch('neptune.internal.hardware.gpu.gpu_monitor.nvmlDeviceGetHandleByIndex')
        nvmlDeviceGetHandleByIndex = patcher.start()
        nvmlDeviceGetHandleByIndex.side_effect = \
            lambda card_index: self.gpu_card_handle if card_index == self.card_index else None
        self.addCleanup(patcher.stop)

    @patch('neptune.internal.hardware.gpu.gpu_monitor.nvmlDeviceGetUtilizationRates')
    def test_gpu_usage_gauge(self, nvmlDeviceGetMemoryInfo):
        # given
        gauge = GpuUsageGauge(card_index=self.card_index)
        # and
        util_info = MagicMock()
        util_info.gpu = 40
        nvmlDeviceGetMemoryInfo.side_effect = lambda handle: util_info if handle == self.gpu_card_handle else None

        # when
        usage_percent = gauge.value()

        # then
        self.assertEqual(40.0, usage_percent)
        self.assertEqual(float, type(usage_percent))

    @patch('neptune.internal.hardware.gpu.gpu_monitor.nvmlDeviceGetMemoryInfo')
    def test_gpu_memory_gauge(self, nvmlDeviceGetMemoryInfo):
        # given
        gauge = GpuMemoryGauge(card_index=self.card_index)
        # and
        memory_info = MagicMock()
        memory_info.used = 3 * BYTES_IN_ONE_GB
        nvmlDeviceGetMemoryInfo.side_effect = lambda handle: memory_info if handle == self.gpu_card_handle else None

        # when
        memory_gb = gauge.value()

        # then
        self.assertEqual(memory_gb, 3.0)
        self.assertEqual(float, type(memory_gb))


if __name__ == '__main__':
    unittest.main()
