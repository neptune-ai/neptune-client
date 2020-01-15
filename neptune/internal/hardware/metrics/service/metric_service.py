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


class MetricService(object):
    def __init__(self, backend, metric_reporter, experiment, metrics_container):
        self.__backend = backend
        self.__metric_reporter = metric_reporter
        self.experiment = experiment
        self.metrics_container = metrics_container

    def report_and_send(self, timestamp):
        metric_reports = self.__metric_reporter.report(timestamp)
        self.__backend.send_hardware_metric_reports(self.experiment, self.metrics_container.metrics(), metric_reports)
