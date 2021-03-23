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
import time
import unittest
import uuid

from mock import ANY, MagicMock, call
import psutil

from neptune.internal.hardware.constants import BYTES_IN_ONE_GB
from neptune.internal.hardware.gauges.cpu import SystemCpuUsageGauge
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.gauges.memory import SystemMemoryUsageGauge
from neptune.internal.hardware.metrics.metric import Metric, MetricResourceType
from neptune.internal.hardware.metrics.reports.metric_report import MetricReport, MetricValue
from neptune.internal.hardware.metrics.service.metric_service_factory import MetricServiceFactory
from neptune.utils import IS_MACOS


class TestMetricServiceIntegration(unittest.TestCase):
    def setUp(self):
        self.backend = MagicMock()
        self.metric_service_factory = MetricServiceFactory(backend=self.backend, os_environ=os.environ)

    @unittest.skipIf(IS_MACOS, "MacOS behaves strangely")
    def test_create_system_metrics(self):
        # given
        memory_amount_gb = psutil.virtual_memory().total / float(BYTES_IN_ONE_GB)

        # and
        experiment = MagicMock()

        # and
        cpu_metric_id = str(uuid.uuid4())
        ram_metric_id = str(uuid.uuid4())
        self.backend.create_hardware_metric.side_effect = [cpu_metric_id, ram_metric_id]

        # when
        self.metric_service_factory.create(
            gauge_mode=GaugeMode.SYSTEM, experiment=experiment, reference_timestamp=time.time())

        # then
        self.backend.create_hardware_metric.assert_has_calls([
            call(
                experiment,
                Metric(
                    internal_id=cpu_metric_id,
                    name=u'CPU - usage',
                    description=u'average of all cores',
                    resource_type=MetricResourceType.CPU,
                    unit=u'%',
                    min_value=0.0,
                    max_value=100.0,
                    gauges=[SystemCpuUsageGauge()]
                )
            ),
            call(
                experiment,
                Metric(
                    internal_id=ram_metric_id,
                    name=u'RAM',
                    description=u'',
                    resource_type=MetricResourceType.RAM,
                    unit=u'GB',
                    min_value=0.0,
                    max_value=memory_amount_gb,
                    gauges=[SystemMemoryUsageGauge()]
                )
            )
        ])

    @unittest.skipIf(IS_MACOS, "MacOS behaves strangely")
    def test_report_and_send_metrics(self):
        # given
        experiment_start = time.time()
        second_after_start = experiment_start + 1.0

        # and
        experiment = MagicMock()

        # and
        metric_service = self.metric_service_factory.create(
            gauge_mode=GaugeMode.SYSTEM, experiment=experiment, reference_timestamp=experiment_start)
        metrics_container = metric_service.metrics_container

        # when
        metric_service.report_and_send(timestamp=second_after_start)

        # then
        self.backend.send_hardware_metric_reports.assert_called_once_with(
            experiment,
            metrics_container.metrics(),
            [
                MetricReport(
                    metric=metrics_container.cpu_usage_metric,
                    values=[MetricValue(timestamp=second_after_start, running_time=1.0, gauge_name=u'cpu', value=ANY)]
                ),
                MetricReport(
                    metric=metrics_container.memory_metric,
                    values=[MetricValue(timestamp=second_after_start, running_time=1.0, gauge_name=u'ram', value=ANY)]
                )
            ]
        )


if __name__ == '__main__':
    unittest.main()
