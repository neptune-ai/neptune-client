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

from neptune.internal.hardware.constants import BYTES_IN_ONE_GB
from neptune.internal.hardware.gauges.cpu import SystemCpuUsageGauge
from neptune.internal.hardware.gauges.gauge_factory import GaugeFactory
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.gauges.gpu import GpuMemoryGauge, GpuUsageGauge
from neptune.internal.hardware.gauges.memory import SystemMemoryUsageGauge
from neptune.internal.hardware.metrics.metric import Metric, MetricResourceType
from neptune.internal.hardware.metrics.metrics_factory import MetricsFactory
from neptune.internal.hardware.resources.system_resource_info import SystemResourceInfo


class TestMetricsFactory(unittest.TestCase):
    def setUp(self):
        self.gauge_factory = GaugeFactory(GaugeMode.SYSTEM)

    def test_create_metrics_with_gpu(self):
        # given
        system_resource_info = SystemResourceInfo(
            cpu_core_count=4,
            memory_amount_bytes=16 * BYTES_IN_ONE_GB,
            gpu_card_indices=[0, 1],
            gpu_memory_amount_bytes=8 * BYTES_IN_ONE_GB
        )
        # and
        metrics_factory = MetricsFactory(self.gauge_factory, system_resource_info)

        # when
        metrics_container = metrics_factory.create_metrics_container()

        # then
        self.assertEqual(
            Metric(
                name=u'CPU - usage',
                description=u'average of all cores',
                resource_type=MetricResourceType.CPU,
                unit=u'%',
                min_value=0.0,
                max_value=100.0,
                gauges=[SystemCpuUsageGauge()]
            ),
            metrics_container.cpu_usage_metric
        )
        # and
        self.assertEqual(
            Metric(
                name=u'RAM',
                description=u'',
                resource_type=MetricResourceType.RAM,
                unit=u'GB',
                min_value=0.0,
                max_value=16.0,
                gauges=[SystemMemoryUsageGauge()]
            ),
            metrics_container.memory_metric
        )
        # and
        self.assertEqual(
            Metric(
                name=u'GPU - usage',
                description=u'2 cards',
                resource_type=MetricResourceType.GPU,
                unit=u'%',
                min_value=0.0,
                max_value=100.0,
                gauges=[GpuUsageGauge(card_index=0), GpuUsageGauge(card_index=1)]
            ),
            metrics_container.gpu_usage_metric
        )
        # and
        self.assertEqual(
            Metric(
                name=u'GPU - memory',
                description=u'2 cards',
                resource_type=MetricResourceType.GPU_RAM,
                unit=u'GB',
                min_value=0.0,
                max_value=8.0,
                gauges=[GpuMemoryGauge(card_index=0), GpuMemoryGauge(card_index=1)]
            ),
            metrics_container.gpu_memory_metric
        )

    def test_create_metrics_without_gpu(self):
        # given
        system_resource_info = SystemResourceInfo(
            cpu_core_count=4,
            memory_amount_bytes=16 * BYTES_IN_ONE_GB,
            gpu_card_indices=[],
            gpu_memory_amount_bytes=0
        )
        # and
        metrics_factory = MetricsFactory(self.gauge_factory, system_resource_info)

        # when
        metrics_container = metrics_factory.create_metrics_container()

        # then
        self.assertIsNotNone(metrics_container.cpu_usage_metric)
        self.assertIsNotNone(metrics_container.memory_metric)
        self.assertIsNone(metrics_container.gpu_usage_metric)
        self.assertIsNone(metrics_container.gpu_memory_metric)

        # and
        self.assertEqual(
            [metrics_container.cpu_usage_metric, metrics_container.memory_metric],
            metrics_container.metrics()
        )

    def test_format_fractional_cpu_core_count(self):
        # given
        system_resource_info = SystemResourceInfo(
            cpu_core_count=0.5,
            memory_amount_bytes=2 * BYTES_IN_ONE_GB,
            gpu_card_indices=[],
            gpu_memory_amount_bytes=0
        )
        # and
        metrics_factory = MetricsFactory(self.gauge_factory, system_resource_info)

        # when
        metrics_container = metrics_factory.create_metrics_container()

        # then
        self.assertEqual(
            Metric(
                name=u'CPU - usage',
                description=u'average of all cores',
                resource_type=MetricResourceType.CPU,
                unit=u'%',
                min_value=0.0,
                max_value=100.0,
                gauges=[SystemCpuUsageGauge()]
            ),
            metrics_container.cpu_usage_metric
        )


if __name__ == '__main__':
    unittest.main()
