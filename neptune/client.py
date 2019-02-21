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
from functools import partial
import io
from io import StringIO
from itertools import groupby
import uuid

from bravado.client import SwaggerClient
from bravado.exception import BravadoConnectionError, BravadoTimeoutError, HTTPForbidden, HTTPInternalServerError, \
    HTTPNotFound, HTTPServerError, HTTPUnauthorized, HTTPUnprocessableEntity, HTTPBadRequest
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat
import requests

from neptune.api_exceptions import ConnectionLost, ExperimentAlreadyFinished, ExperimentLimitReached, \
    ExperimentNotFound, Forbidden, OrganizationNotFound, ProjectNotFound, ServerError, StorageLimitReached, \
    Unauthorized, DuplicateParameter, InvalidTag
from neptune.experiments import Experiment
from neptune.model import ChannelWithLastValue, LeaderboardEntry
from neptune.oauth import NeptuneAuthenticator
from neptune.utils import is_float


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BravadoConnectionError, BravadoTimeoutError,
                requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            raise ConnectionLost()
        except HTTPServerError:
            raise ServerError()
        except HTTPUnauthorized:
            raise Unauthorized()
        except HTTPForbidden:
            raise Forbidden()
        except requests.exceptions.RequestException as e:
            if e.response is None:
                raise
            status_code = e.response.status_code
            if status_code >= HTTPInternalServerError.status_code:
                raise ServerError()
            elif status_code == HTTPUnauthorized.status_code:
                raise Unauthorized()
            elif status_code == HTTPForbidden.status_code:
                raise Forbidden()
            else:
                raise

    return wrapper


class Client(object):
    @with_api_exceptions_handler
    def __init__(self, api_address, api_token):
        self.api_address = api_address
        self.api_token = api_token

        self._http_client = RequestsClient()

        self.backend_swagger_client = SwaggerClient.from_url(
            '{}/api/backend/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                formats=[uuid_format]),
            http_client=self._http_client)

        self.leaderboard_swagger_client = SwaggerClient.from_url(
            '{}/api/leaderboard/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                validate_responses=False,  # TODO!!!
                formats=[uuid_format]),
            http_client=self._http_client)

        self.authenticator = NeptuneAuthenticator(
            self.backend_swagger_client.api.exchangeApiToken(X_Neptune_Api_Token=api_token).response().result
        )
        self._http_client.authenticator = self.authenticator

    def get_project(self, project_qualified_name):
        try:
            return self.backend_swagger_client.api.getProject(
                projectIdentifier=project_qualified_name
            ).response().result
        except HTTPNotFound:
            raise ProjectNotFound(project_qualified_name)

    @with_api_exceptions_handler
    def get_projects(self, namespace):
        try:
            r = self.backend_swagger_client.api.listProjectsInOrganization(
                organizationName=namespace
            ).response()
            return r.result.entries
        except HTTPNotFound:
            raise OrganizationNotFound(organization_name=namespace)

    @with_api_exceptions_handler
    def get_project_members(self, project_identifier):
        try:
            r = self.backend_swagger_client.api.listProjectMembers(projectIdentifier=project_identifier).response()
            return r.result
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier)

    @with_api_exceptions_handler
    def get_leaderboard_entries(self, project,
                                entry_types=None, ids=None, group_ids=None,
                                states=None, owners=None, tags=None,
                                min_running_time=None):
        try:
            if entry_types is None:
                entry_types = ['experiment', 'notebook']

            def get_portion(limit, offset):
                return self.leaderboard_swagger_client.api.getLeaderboard(
                    projectIdentifier=project.full_id,
                    entryType=entry_types,
                    shortId=ids, groupShortId=group_ids, state=states, owner=owners, tags=tags,
                    minRunningTimeSeconds=min_running_time,
                    sortBy=['shortId'], sortFieldType=['native'], sortDirection=['ascending'],
                    limit=limit, offset=offset
                ).response().result.entries

            return [LeaderboardEntry(e) for e in self._get_all_items(get_portion, step=100)]
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

    @with_api_exceptions_handler
    def get_channel_points_csv(self, experiment, channel_internal_id):
        try:
            csv = StringIO()
            csv.write(
                self.backend_swagger_client.api.getChannelValuesCSV(
                    experimentId=experiment.internal_id, channelId=channel_internal_id
                ).response().incoming_response.text
            )
            csv.seek(0)
            return csv
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def get_metrics_csv(self, experiment):
        try:
            csv = StringIO()
            csv.write(
                self.backend_swagger_client.api.getSystemMetricsCSV(
                    experimentId=experiment.internal_id
                ).response().incoming_response.text
            )
            csv.seek(0)
            return csv
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def create_experiment(self, project, name, description, params, properties, tags, abortable, monitored):
        ExperimentCreationParams = self.backend_swagger_client.get_model('ExperimentCreationParams')

        try:
            params = ExperimentCreationParams(
                projectId=project.internal_id,
                name=name,
                description=description,
                parameters=self._convert_to_api_parameters(params),
                properties=self._convert_to_api_properties(properties),
                tags=tags,
                enqueueCommand="command",  # FIXME
                entrypoint="",  # FIXME
                execArgsTemplate="",  # FIXME,
                abortable=abortable,
                monitored=monitored
            )

            experiment = self.backend_swagger_client.api.createExperiment(
                experimentCreationParams=params).response().result

            return self._convert_experiment_to_leaderboard_entry(experiment)
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)
        except HTTPBadRequest as e:
            error_response = e.response.json()
            error_type = error_response.get('type')
            if error_type == 'DUPLICATE_PARAMETER':
                raise DuplicateParameter()
            elif error_type == 'INVALID_TAG':
                raise InvalidTag(error_response.get('message'))
            else:
                raise
        except HTTPUnprocessableEntity as e:
            if e.response.json().get('type') == 'LIMIT_OF_EXPERIMENTS_IN_PROJECT_REACHED':
                raise ExperimentLimitReached()
            else:
                raise

    @with_api_exceptions_handler
    def upload_experiment_source(self, experiment, data):
        try:
            return self._upload_loop(
                partial(self._upload_raw_data, api_method=self.backend_swagger_client.api.uploadExperimentSource),
                data=data,
                experiment=experiment)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity as e:
            if e.response.json().get('type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
                raise StorageLimitReached()
            else:
                raise

    @with_api_exceptions_handler
    def extract_experiment_source(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentSourceAsTarstream,
                data=data
            )
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity as e:
            if e.response.json().get('type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
                raise StorageLimitReached()
            else:
                raise

    @with_api_exceptions_handler
    def mark_waiting(self, experiment):
        try:
            return self._convert_experiment_to_leaderboard_entry(
                self.backend_swagger_client.api.markExperimentWaiting(
                    experimentId=experiment.internal_id).response().result
            )
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def mark_initializing(self, experiment):
        try:
            return self._convert_experiment_to_leaderboard_entry(
                self.backend_swagger_client.api.markExperimentInitializing(
                    experimentId=experiment.internal_id).response().result
            )
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def mark_running(self, experiment):
        RunningExperimentParams = self.backend_swagger_client.get_model('RunningExperimentParams')

        try:
            params = RunningExperimentParams(
                runCommand=""  # FIXME
            )

            experiment = self.backend_swagger_client.api.markExperimentRunning(
                experimentId=experiment.internal_id,
                runningExperimentParams=params
            ).response().result

            return self._convert_experiment_to_leaderboard_entry(experiment)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def create_channel(self, experiment, name, channel_type):
        ChannelParams = self.backend_swagger_client.get_model('ChannelParams')

        try:
            params = ChannelParams(
                name=name,
                channelType=channel_type
            )

            channel = self.backend_swagger_client.api.createChannel(
                experimentId=experiment.internal_id,
                channelToCreate=params
            ).response().result

            return self._convert_channel_to_channel_with_last_value(channel)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def send_channel_value(self, experiment, channel_id, x, y):
        InputChannelValues = self.backend_swagger_client.get_model('InputChannelValues')
        Point = self.backend_swagger_client.get_model('Point')
        Y = self.backend_swagger_client.get_model('Y')

        try:
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
                experimentId=experiment.internal_id,
                channelsValues=[values]
            ).response().result

            if batch_errors:
                raise ValueError(batch_errors[0].error.message)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def mark_succeeded(self, experiment):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        try:
            experiment = self.backend_swagger_client.api.markExperimentCompleted(
                experimentId=experiment.internal_id,
                completedExperimentParams=CompletedExperimentParams(
                    state='succeeded',
                    traceback=''  # FIXME
                )
            ).response().result

            return self._convert_experiment_to_leaderboard_entry(experiment)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity:
            raise ExperimentAlreadyFinished(experiment.id)

    @with_api_exceptions_handler
    def mark_failed(self, experiment, traceback):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        try:
            experiment = self.backend_swagger_client.api.markExperimentCompleted(
                experimentId=experiment.internal_id,
                completedExperimentParams=CompletedExperimentParams(
                    state='failed',
                    traceback=traceback
                )
            ).response().result

            return self._convert_experiment_to_leaderboard_entry(experiment)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity:
            raise ExperimentAlreadyFinished(experiment.id)

    @with_api_exceptions_handler
    def ping_experiment(self, experiment):
        try:
            self.backend_swagger_client.api.pingExperiment(experimentId=experiment.internal_id).response()
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def create_hardware_metric(self, experiment, metric):
        SystemMetricParams = self.backend_swagger_client.get_model('SystemMetricParams')

        try:
            series = [gauge.name() for gauge in metric.gauges]
            system_metric_params = SystemMetricParams(
                name=metric.name, description=metric.description, resourceType=metric.resource_type,
                unit=metric.unit, min=metric.min_value, max=metric.max_value, series=series)

            metric_dto = self.backend_swagger_client.api.createSystemMetric(
                experimentId=experiment.internal_id, metricToCreate=system_metric_params
            ).response().result

            return metric_dto.id
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def send_hardware_metric_reports(self, experiment, metrics, metric_reports):
        SystemMetricValues = self.backend_swagger_client.get_model('SystemMetricValues')
        SystemMetricPoint = self.backend_swagger_client.get_model('SystemMetricPoint')

        try:
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
                experimentId=experiment.internal_id, metricValues=system_metric_values).response()

            return response
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)

    @with_api_exceptions_handler
    def upload_experiment_output(self, experiment, data):
        try:
            return self._upload_loop(
                partial(self._upload_raw_data, api_method=self.backend_swagger_client.api.uploadExperimentOutput),
                data=data,
                experiment=experiment)
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity as e:
            if e.response.json().get('type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
                raise StorageLimitReached()
            else:
                raise

    @with_api_exceptions_handler
    def extract_experiment_output(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentOutputAsTarstream,
                data=data
            )
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment.project_full_id)
        except HTTPUnprocessableEntity as e:
            if e.response.json().get('type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
                raise StorageLimitReached()
            else:
                raise

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

    def _upload_loop(self, fun, data, checksums=None, **kwargs):
        ret = None
        for part in data.generate():
            skip = False
            if checksums and part.start in checksums:
                skip = checksums[part.start].checksum == part.md5()

            if not skip:
                ret = self._upload_loop_chunk(fun, part, data, **kwargs)
            else:
                part.skip()
        data.close()
        return ret

    def _upload_loop_chunk(self, fun, part, data, **kwargs):
        part_to_send = part.get_data()
        if part.end:
            binary_range = "bytes=%d-%d/%d" % (part.start, part.end - 1, data.length)
        else:
            binary_range = "bytes=%d-/%d" % (part.start, data.length)
        return fun(data=part_to_send,
                   headers={
                       "Content-Type": "application/octet-stream",
                       "Content-Filename": data.filename,
                       "Range": binary_range,
                       "X-File-Permissions": data.permissions
                   },
                   **kwargs)

    def _upload_raw_data(self, experiment, api_method, data, headers):
        url = self.api_address + api_method.operation.path_name
        url = url.replace("{experimentId}", experiment.internal_id)

        session = self._http_client.session

        request = self.authenticator.apply(
            requests.Request(
                method='POST',
                url=url,
                data=data,
                headers=headers
            )
        )

        return session.send(session.prepare_request(request))

    def _upload_tar_data(self, experiment, api_method, data):
        url = self.api_address + api_method.operation.path_name
        url = url.replace("{experimentId}", experiment.internal_id)

        session = self._http_client.session

        request = self.authenticator.apply(
            requests.Request(
                method='POST',
                url=url,
                data=io.BytesIO(data),
                headers={
                    "Content-Type": "application/octet-stream"
                }
            )
        )

        return session.send(session.prepare_request(request))


uuid_format = SwaggerFormat(
    format='uuid',
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description=''
)
