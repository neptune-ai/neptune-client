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
from io import StringIO
from itertools import groupby
import uuid

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat

from neptune.experiment import Experiment
from neptune.model import LeaderboardEntry, ChannelWithLastValue
from neptune.oauth import NeptuneAuthenticator
from neptune.utils import is_float


class Client(object):
    def __init__(self, api_address, api_token):
        self.api_address = api_address
        self.api_token = api_token

        http_client = RequestsClient()

        self.backend_swagger_client = SwaggerClient.from_url(
            '{}/api/backend/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                formats=[uuid_format]),
            http_client=http_client)

        self.leaderboard_swagger_client = SwaggerClient.from_url(
            '{}/api/leaderboard/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                validate_responses=False,  # TODO!!!
                formats=[uuid_format]),
            http_client=http_client)

        http_client.authenticator = NeptuneAuthenticator(
            self.backend_swagger_client.api.exchangeApiToken(X_Neptune_Api_Token=api_token).response().result)

    def get_project(self, organization_name, project_name):
        r = self.backend_swagger_client.api.getProjectByName(
            organizationName=organization_name,
            projectName=project_name
        ).response()

        return r.result

    def get_projects(self, namespace):
        r = self.backend_swagger_client.api.listProjectsInOrganization(
            organizationName=namespace
        ).response()
        return r.result.entries

    def get_project_members(self, project_identifier):
        r = self.backend_swagger_client.api.listProjectMembers(
            projectIdentifier=project_identifier
        ).response()

        return r.result

    def get_leaderboard_entries(self, namespace, project_name,
                                entry_types=None, ids=None, group_ids=None,
                                states=None, owners=None, tags=None,
                                min_running_time=None):
        if entry_types is None:
            entry_types = ['experiment', 'notebook']

        def get_portion(limit, offset):
            return self.leaderboard_swagger_client.api.getLeaderboard(
                projectIdentifier="{}/{}".format(namespace, project_name),
                entryType=entry_types,
                shortId=ids, groupShortId=group_ids, state=states, owner=owners, tags=tags,
                minRunningTimeSeconds=min_running_time,
                sortBy=['shortId'], sortFieldType=['native'], sortDirection=['ascending'],
                limit=limit, offset=offset
            ).response().result.entries

        return [LeaderboardEntry(e) for e in self._get_all_items(get_portion, step=100)]

    def get_channel_points_csv(self, experiment_internal_id, channel_internal_id):
        csv = StringIO()
        csv.write(
            self.backend_swagger_client.api.getChannelValuesCSV(
                experimentId=experiment_internal_id, channelId=channel_internal_id
            ).response().incoming_response.text
        )
        csv.seek(0)
        return csv

    def get_metrics_csv(self, experiment_internal_id):
        csv = StringIO()
        csv.write(
            self.backend_swagger_client.api.getSystemMetricsCSV(
                experimentId=experiment_internal_id
            ).response().incoming_response.text
        )
        csv.seek(0)
        return csv

    def create_experiment(self, project_id, name, description, params, properties, tags):
        ExperimentCreationParams = self.backend_swagger_client.get_model('ExperimentCreationParams')

        params = ExperimentCreationParams(
            projectId=project_id,
            name=name,
            description=description,
            parameters=self._convert_to_api_parameters(params),
            properties=self._convert_to_api_properties(properties),
            tags=tags,
            enqueueCommand="command",  # FIXME
            entrypoint="",  # FIXME
            execArgsTemplate=""  # FIXME
        )

        experiment = self.backend_swagger_client.api.createExperiment(experimentCreationParams=params).response().result

        return self._convert_experiment_to_leaderboard_entry(experiment)

    def mark_waiting(self, experiment_id):
        return self._convert_experiment_to_leaderboard_entry(
            self.backend_swagger_client.api.markExperimentWaiting(experimentId=experiment_id).response().result
        )

    def mark_initializing(self, experiment_id):
        return self._convert_experiment_to_leaderboard_entry(
            self.backend_swagger_client.api.markExperimentInitializing(experimentId=experiment_id).response().result
        )

    def mark_running(self, experiment_id):
        RunningExperimentParams = self.backend_swagger_client.get_model('RunningExperimentParams')

        params = RunningExperimentParams(
            runCommand=""  # FIXME
        )

        experiment = self.backend_swagger_client.api.markExperimentRunning(
            experimentId=experiment_id,
            runningExperimentParams=params
        ).response().result

        return self._convert_experiment_to_leaderboard_entry(experiment)

    def create_channel(self, experiment_id, name, channel_type):
        ChannelParams = self.backend_swagger_client.get_model('ChannelParams')

        params = ChannelParams(
            name=name,
            channelType=channel_type
        )

        channel = self.backend_swagger_client.api.createChannel(
            experimentId=experiment_id,
            channelToCreate=params
        ).response().result

        return self._convert_channel_to_channel_with_last_value(channel)

    def send_channel_value(self, experiment_id, channel_id, x, y):
        InputChannelValues = self.backend_swagger_client.get_model('InputChannelValues')
        Point = self.backend_swagger_client.get_model('Point')
        Y = self.backend_swagger_client.get_model('Y')

        values = InputChannelValues(
            channelId=channel_id,
            values=[Point(
                x=x,
                y=Y(
                    numericValue=y.get('numeric_value'),
                    textValue=y.get('text_value'),
                    inputImageValue=y.get('image_value')
                )
            )]
        )

        batch_errors = self.backend_swagger_client.api.postChannelValues(
            experimentId=experiment_id,
            channelsValues=[values]
        ).response().result

        if batch_errors:
            raise ValueError(batch_errors[0].error.message)

    def mark_succeeded(self, experiment_id):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        experiment = self.backend_swagger_client.api.markExperimentCompleted(
            experimentId=experiment_id,
            completedExperimentParams=CompletedExperimentParams(
                state='succeeded',
                traceback=''  # FIXME
            )
        ).response().result

        return self._convert_experiment_to_leaderboard_entry(experiment)

    def mark_failed(self, experiment_id, traceback):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        experiment = self.backend_swagger_client.api.markExperimentCompleted(
            experimentId=experiment_id,
            completedExperimentParams=CompletedExperimentParams(
                state='failed',
                traceback=traceback
            )
        ).response().result

        return self._convert_experiment_to_leaderboard_entry(experiment)

    def ping_experiment(self, experiment_id):
        self.backend_swagger_client.api.pingExperiment(experimentId=experiment_id).response()

    def create_hardware_metric(self, experiment_id, metric):
        SystemMetricParams = self.backend_swagger_client.get_model('SystemMetricParams')

        series = [gauge.name() for gauge in metric.gauges]
        system_metric_params = SystemMetricParams(
            name=metric.name, description=metric.description, resourceType=metric.resource_type,
            unit=metric.unit, min=metric.min_value, max=metric.max_value, series=series)

        metric_dto = self.backend_swagger_client.api.createSystemMetric(
            experimentId=experiment_id, metricToCreate=system_metric_params
        ).response().result

        return metric_dto.id

    def send_hardware_metric_reports(self, experiment_id, metrics, metric_reports):
        SystemMetricValues = self.backend_swagger_client.get_model('SystemMetricValues')
        SystemMetricPoint = self.backend_swagger_client.get_model('SystemMetricPoint')

        metrics_by_name = {metric.name: metric for metric in metrics}

        system_metric_values = [
            SystemMetricValues(
                metricId=metrics_by_name.get(report.metric.name).internal_id,
                seriesName=gauge_name,
                values=[
                    SystemMetricPoint(x=int(metric_value.timestamp * 1000.0), y=metric_value.value)
                    for metric_value in metric_values
                ]
            )
            for report in metric_reports
            for gauge_name, metric_values in groupby(report.values, lambda value: value.gauge_name)
        ]

        response = self.backend_swagger_client.api.postSystemMetricValues(
            experimentId=experiment_id, metricValues=system_metric_values).response()

        return response

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []

        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items

        return items

    def _convert_to_api_parameters(self, raw_params):
        Parameter = self.backend_swagger_client.get_model('Parameter')

        params = []
        for name, value in raw_params.items():
            parameter_type = 'double' if is_float(value) else 'string'

            params.append(
                Parameter(
                    id=str(uuid.uuid4()),
                    name=name,
                    parameterType=parameter_type,
                    value=str(value)
                )
            )

        return params

    def _convert_to_api_properties(self, raw_properties):
        KeyValueProperty = self.backend_swagger_client.get_model('KeyValueProperty')

        return [
            KeyValueProperty(
                key=key,
                value=value
            ) for key, value in raw_properties.items()
        ]

    def _convert_experiment_to_leaderboard_entry(self, experiment):
        LeaderboardEntryDTO = self.leaderboard_swagger_client.get_model('LeaderboardEntryDTO')

        experiment_states = {"creating": 0, "waiting": 0, "initializing": 0, "running": 0, "cleaning": 0,
                             "succeeded": 0, "aborted": 0, "failed": 0, "crashed": 0, "preempted": 0,
                             experiment.state: 1}

        return Experiment(
            client=self,
            leaderboard_entry=LeaderboardEntry(
                LeaderboardEntryDTO(
                    id=experiment.id,
                    shortId=experiment.shortId,
                    name=experiment.name,
                    organizationId=experiment.organizationId,
                    organizationName=experiment.organizationName,
                    projectId=experiment.projectId,
                    projectName=experiment.projectName,
                    timeOfCreation=experiment.timeOfCreation,
                    description=experiment.description,
                    entryType="experiment",
                    state=experiment.state,
                    tags=experiment.tags,
                    channelsLastValues=experiment.channelsLastValues,
                    experimentStates=experiment_states,
                    owner=experiment.owner,
                    parameters=experiment.parameters,
                    properties=experiment.properties
                )
            )
        )

    def _convert_channel_to_channel_with_last_value(self, channel):
        ChannelWithValueDTO = self.leaderboard_swagger_client.get_model('ChannelWithValueDTO')
        return ChannelWithLastValue(
            ChannelWithValueDTO(
                channelId=channel.id,
                channelName=channel.name,
                channelType=channel.channelType,
                x=None,
                y=None
            )
        )


uuid_format = SwaggerFormat(
    format='uuid',
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description=''
)
