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

from neptune.internal.hardware.constants import BYTES_IN_ONE_GB
from neptune.internal.hardware.metrics.metrics_factory import MetricsFactory
from neptune.internal.hardware.metrics.reports.metric_report import MetricReport, MetricValue
from neptune.internal.hardware.metrics.reports.metric_reporter_factory import MetricReporterFactory
from neptune.internal.hardware.resources.system_resource_info import SystemResourceInfo
from tests.neptune.internal.hardware.gauges.gauges_fixture import GaugesFixture


class TestMetricReporterIntegration(unittest.TestCase):
    def setUp(self):
        self.maxDiff = 65536

        self.fixture = GaugesFixture()
        self.metrics_container = MetricsFactory(
            gauge_factory=self.fixture.gauge_factory,
            system_resource_info=SystemResourceInfo(
                cpu_core_count=4,
                memory_amount_bytes=64 * BYTES_IN_ONE_GB,
                gpu_card_indices=[0, 2],
                gpu_memory_amount_bytes=32 * BYTES_IN_ONE_GB
            )
        ).create_metrics_container()

        self.reference_timestamp = time.time()
        metric_reporter_factory = MetricReporterFactory(reference_timestamp=self.reference_timestamp)
        self.metric_reporter = metric_reporter_factory.create(self.metrics_container.metrics())

    def test_report_metrics(self):
        # given
        measurement_timestamp = self.reference_timestamp + 10

        # when
        metric_reports = self.metric_reporter.report(measurement_timestamp)

        # then
        expected_time = measurement_timestamp - self.reference_timestamp
        expected_reports = [
            MetricReport(
                metric=self.metrics_container.cpu_usage_metric,
                values=[MetricValue(
                    timestamp=measurement_timestamp,
                    running_time=expected_time,
                    gauge_name=u'cpu',
                    value=self.fixture.cpu_gauge_value
                )]
            ),
            MetricReport(
                metric=self.metrics_container.memory_metric,
                values=[MetricValue(
                    timestamp=measurement_timestamp,
                    running_time=expected_time,
                    gauge_name=u'ram',
                    value=self.fixture.memory_gauge_value
                )]
            ),
            MetricReport(
                metric=self.metrics_container.gpu_usage_metric,
                values=[
                    MetricValue(
                        timestamp=measurement_timestamp,
                        running_time=expected_time,
                        gauge_name=u'0',
                        value=self.fixture.gpu0_usage_gauge_value
                    ),
                    MetricValue(
                        timestamp=measurement_timestamp,
                        running_time=expected_time,
                        gauge_name=u'2',
                        value=self.fixture.gpu1_usage_gauge_value
                    )
                ]
            ),
            MetricReport(
                metric=self.metrics_container.gpu_memory_metric,
                values=[
                    MetricValue(
                        timestamp=measurement_timestamp,
                        running_time=expected_time,
                        gauge_name=u'0',
                        value=self.fixture.gpu0_memory_gauge_value
                    ),
                    MetricValue(
                        timestamp=measurement_timestamp,
                        running_time=expected_time,
                        gauge_name=u'2',
                        value=self.fixture.gpu1_memory_gauge_value
                    )
                ]
            )
        ]
        self.assertListEqual(expected_reports, metric_reports)


if __name__ == '__main__':
    unittest.main()
