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
import base64
import gzip
import io
import os
import uuid
from functools import partial

from http.client import NOT_FOUND, UNPROCESSABLE_ENTITY
from io import StringIO
from itertools import groupby

import requests
import six
import urllib3

from bravado.client import SwaggerClient
from bravado.exception import BravadoConnectionError, BravadoTimeoutError, HTTPBadRequest, HTTPForbidden, \
    HTTPInternalServerError, HTTPNotFound, HTTPServerError, HTTPUnauthorized, HTTPUnprocessableEntity, HTTPConflict
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat

from neptune.api_exceptions import ConnectionLost, ExperimentAlreadyFinished, ExperimentLimitReached, \
    ExperimentNotFound, ExperimentValidationError, Forbidden, NamespaceNotFound, ProjectNotFound, ServerError, \
    StorageLimitReached, Unauthorized, ChannelAlreadyExists, ChannelsValuesSendBatchError, SSLError, NotebookNotFound
from neptune.checkpoint import Checkpoint
from neptune.experiments import Experiment
from neptune.internal.utils.http import extract_response_field
from neptune.model import ChannelWithLastValue, LeaderboardEntry
from neptune.notebook import Notebook
from neptune.oauth import NeptuneAuthenticator
from neptune.utils import is_float


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.SSLError:
            raise SSLError()
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
    def __init__(self, api_address, api_token, proxies=None):
        self.api_address = api_address
        self.api_token = api_token
        self.proxies = proxies
        ssl_verify = True
        if os.getenv("NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = RequestsClient(ssl_verify=ssl_verify)
        if self.proxies is not None:
            self._update_proxies()

        self.backend_swagger_client = SwaggerClient.from_url(
            '{}/api/backend/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                validate_requests=False,
                validate_responses=False,
                formats=[uuid_format]),
            http_client=self._http_client)

        self.leaderboard_swagger_client = SwaggerClient.from_url(
            '{}/api/leaderboard/swagger.json'.format(self.api_address),
            config=dict(
                validate_swagger_spec=False,
                validate_requests=False,
                validate_responses=False,
                formats=[uuid_format]
            ),
            http_client=self._http_client
        )

        self.authenticator = NeptuneAuthenticator(
            self.backend_swagger_client.api.exchangeApiToken(X_Neptune_Api_Token=api_token).response().result
        )
        self._http_client.authenticator = self.authenticator

        # This is not a top-level import because of circular dependencies
        from neptune import __version__
        self.client_lib_version = __version__

    @with_api_exceptions_handler
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
            r = self.backend_swagger_client.api.listProjects(
                organizationIdentifier=namespace
            ).response()
            return r.result.entries
        except HTTPNotFound:
            raise NamespaceNotFound(namespace_name=namespace)

    @with_api_exceptions_handler
    def get_project_members(self, project_identifier):
        try:
            r = self.backend_swagger_client.api.listProjectMembers(projectIdentifier=project_identifier).response()
            return r.result
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier)

    @with_api_exceptions_handler
    def get_leaderboard_entries(self, project,
                                entry_types=None, ids=None,
                                states=None, owners=None, tags=None,
                                min_running_time=None):
        try:
            if entry_types is None:
                entry_types = ['experiment', 'notebook']

            def get_portion(limit, offset):
                return self.leaderboard_swagger_client.api.getLeaderboard(
                    projectIdentifier=project.full_id,
                    entryType=entry_types,
                    shortId=ids, groupShortId=None, state=states, owner=owners, tags=tags,
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
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

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
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

    @with_api_exceptions_handler
    def create_experiment(self,
                          project,
                          name,
                          description,
                          params,
                          properties,
                          tags,
                          abortable,
                          monitored,
                          git_info,
                          hostname):
        ExperimentCreationParams = self.backend_swagger_client.get_model('ExperimentCreationParams')
        GitInfoDTO = self.backend_swagger_client.get_model('GitInfoDTO')
        GitCommitDTO = self.backend_swagger_client.get_model('GitCommitDTO')

        git_info_data = None
        if git_info is not None:
            git_info_data = GitInfoDTO(
                commit=GitCommitDTO(
                    commitId=git_info.commit_id,
                    message=git_info.message,
                    authorName=git_info.author_name,
                    authorEmail=git_info.author_email,
                    commitDate=git_info.commit_date
                ),
                repositoryDirty=git_info.repository_dirty
            )

        try:
            params = ExperimentCreationParams(
                projectId=project.internal_id,
                name=name,
                description=description,
                parameters=self._convert_to_api_parameters(params),
                properties=self._convert_to_api_properties(properties),
                tags=tags,
                gitInfo=git_info_data,
                enqueueCommand="command",  # FIXME
                entrypoint="",  # FIXME
                execArgsTemplate="",  # FIXME,
                abortable=abortable,
                monitored=monitored,
                hostname=hostname
            )

            kwargs = {
                'experimentCreationParams': params,
                'X-Neptune-CliVersion': self.client_lib_version
            }
            api_experiment = self.backend_swagger_client.api.createExperiment(**kwargs).response().result

            return self._convert_to_experiment(api_experiment)
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)
        except HTTPBadRequest as e:
            error_type = extract_response_field(e.response, 'type')
            if error_type == 'DUPLICATE_PARAMETER':
                raise ExperimentValidationError('Parameter list contains duplicates.')
            elif error_type == 'INVALID_TAG':
                raise ExperimentValidationError(extract_response_field(e.response, 'message'))
            else:
                raise
        except HTTPUnprocessableEntity as e:
            if extract_response_field(e.response, 'type') == 'LIMIT_OF_EXPERIMENTS_IN_PROJECT_REACHED':
                raise ExperimentLimitReached()
            else:
                raise

    @with_api_exceptions_handler
    def get_notebook(self, project, notebook_id):
        try:
            api_notebook_list = self.leaderboard_swagger_client.api.listNotebooks(
                projectIdentifier=project.internal_id,
                id=[notebook_id]
            ).response().result

            if not api_notebook_list.entries:
                raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

            api_notebook = api_notebook_list.entries[0]

            return Notebook(
                client=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner
            )
        except HTTPNotFound:
            raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

    def get_last_checkpoint(self, project, notebook_id):
        try:
            api_checkpoint_list = self.leaderboard_swagger_client.api.listCheckpoints(
                notebookId=notebook_id,
                offset=0,
                limit=1
            ).response().result

            if not api_checkpoint_list.entries:
                raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

            checkpoint = api_checkpoint_list.entries[0]
            return Checkpoint(checkpoint.id, checkpoint.name, checkpoint.path)
        except HTTPNotFound:
            raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

    @with_api_exceptions_handler
    def create_notebook(self, project):
        try:
            api_notebook = self.leaderboard_swagger_client.api.createNotebook(
                projectIdentifier=project.internal_id
            ).response().result

            return Notebook(
                client=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id, jupyter_path, _file):
        with self._upload_raw_data(
                # pylint: disable=bad-continuation
                api_method=self.leaderboard_swagger_client.api.createCheckpoint,
                data=_file,
                headers={"Content-Type": "application/octet-stream"},
                path_params={
                    "notebookId": notebook_id
                },
                query_params={
                    "jupyterPath": jupyter_path
                }
        ) as response:
            if response.status_code == NOT_FOUND:
                raise NotebookNotFound(notebook_id=notebook_id)
            else:
                response.raise_for_status()

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        return self.backend_swagger_client.api.getExperiment(experimentId=experiment_id).response().result

    @with_api_exceptions_handler
    def update_experiment(self, experiment, properties):
        EditExperimentParams = self.backend_swagger_client.get_model('EditExperimentParams')
        KeyValueProperty = self.backend_swagger_client.get_model('KeyValueProperty')
        try:
            self.backend_swagger_client.api.updateExperiment(
                experimentId=experiment.internal_id,
                editExperimentParams=EditExperimentParams(
                    properties=[KeyValueProperty(
                        key=key,
                        value=properties[key]
                    ) for key in properties]
                )
            ).response()
            return experiment
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project_full_id
            )

    @with_api_exceptions_handler
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        UpdateTagsParams = self.backend_swagger_client.get_model('UpdateTagsParams')
        try:
            self.backend_swagger_client.api.updateTags(
                updateTagsParams=UpdateTagsParams(
                    experimentIds=[experiment.internal_id],
                    groupsIds=[],
                    tagsToAdd=tags_to_add,
                    tagsToDelete=tags_to_delete
                )
            ).response().result
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project_full_id
            )
        except HTTPBadRequest as e:
            error_type = extract_response_field(e.response, 'type')
            if error_type == 'INVALID_TAG':
                raise ExperimentValidationError(extract_response_field(e.response, 'message'))
            else:
                raise

    @with_api_exceptions_handler
    def upload_experiment_source(self, experiment, data):
        with self._upload_loop(
                # pylint: disable=bad-continuation
                partial(self._upload_raw_data, api_method=self.backend_swagger_client.api.uploadExperimentSource),
                data=data,
                path_params={'experimentId': experiment.internal_id},
                query_params={}
        ) as response:
            if response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
            elif (response.status_code == UNPROCESSABLE_ENTITY
                  and extract_response_field(response.content, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
            else:
                response.raise_for_status()

    @with_api_exceptions_handler
    def extract_experiment_source(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentSourceAsTarstream,
                data=data
            )
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPUnprocessableEntity as e:
            if extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
                raise StorageLimitReached()
            else:
                raise

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
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPConflict:
            raise ChannelAlreadyExists(channel_name=name, experiment_short_id=experiment.id)

    @with_api_exceptions_handler
    def create_system_channel(self, experiment, name, channel_type):
        ChannelParams = self.backend_swagger_client.get_model('ChannelParams')

        try:
            params = ChannelParams(
                name=name,
                channelType=channel_type
            )

            channel = self.backend_swagger_client.api.createSystemChannel(
                experimentId=experiment.internal_id,
                channelToCreate=params
            ).response().result

            return self._convert_channel_to_channel_with_last_value(channel)
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPConflict:
            raise ChannelAlreadyExists(channel_name=name, experiment_short_id=experiment.id)

    @with_api_exceptions_handler
    def get_system_channels(self, experiment):
        try:
            channels = self.backend_swagger_client.api.getSystemChannels(
                experimentId=experiment.internal_id,
            ).response().result

            return channels
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

    @with_api_exceptions_handler
    def send_channels_values(self, experiment, channels_with_values):
        InputChannelValues = self.backend_swagger_client.get_model('InputChannelValues')
        Point = self.backend_swagger_client.get_model('Point')
        Y = self.backend_swagger_client.get_model('Y')

        input_channels_values = []
        for channel_with_values in channels_with_values:
            points = [Point(
                timestampMillis=int(value.ts * 1000.0),
                x=value.x,
                y=Y(numericValue=value.y.get('numeric_value'),
                    textValue=value.y.get('text_value'),
                    inputImageValue=value.y.get('image_value'))
            ) for value in channel_with_values.channel_values]

            input_channels_values.append(InputChannelValues(
                channelId=channel_with_values.channel_id,
                values=points
            ))

        try:
            batch_errors = self.backend_swagger_client.api.postChannelValues(
                experimentId=experiment.internal_id,
                channelsValues=input_channels_values
            ).response().result

            if batch_errors:
                raise ChannelsValuesSendBatchError(experiment.id, batch_errors)
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

    @with_api_exceptions_handler
    def put_tensorflow_graph(self, experiment, graph_id, graph):

        TensorflowGraph = self.backend_swagger_client.get_model('TensorflowGraph')

        def gzip_compress(data):
            output_buffer = io.BytesIO()
            gzip_stream = gzip.GzipFile(fileobj=output_buffer, mode='w')
            gzip_stream.write(data)
            gzip_stream.close()
            return output_buffer.getvalue()

        bingraph = graph.encode('UTF-8')
        compressed_graph_data = base64.b64encode(gzip_compress(bingraph))
        data = compressed_graph_data.decode('UTF-8')

        value = TensorflowGraph(id=graph_id, value=data)

        try:
            r = self.backend_swagger_client.api.putTensorflowGraph(
                experimentId=experiment.internal_id,
                tensorflowGraph=value
            ).response()
            return r.result
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

    @with_api_exceptions_handler
    def mark_succeeded(self, experiment):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        try:
            self.backend_swagger_client.api.markExperimentCompleted(
                experimentId=experiment.internal_id,
                completedExperimentParams=CompletedExperimentParams(
                    state='succeeded',
                    traceback=''  # FIXME
                )
            ).response()

            return experiment
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPUnprocessableEntity:
            raise ExperimentAlreadyFinished(experiment.id)

    @with_api_exceptions_handler
    def mark_failed(self, experiment, traceback):
        CompletedExperimentParams = self.backend_swagger_client.get_model('CompletedExperimentParams')

        try:
            self.backend_swagger_client.api.markExperimentCompleted(
                experimentId=experiment.internal_id,
                completedExperimentParams=CompletedExperimentParams(
                    state='failed',
                    traceback=traceback
                )
            ).response()

            return experiment
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPUnprocessableEntity:
            raise ExperimentAlreadyFinished(experiment.id)

    @with_api_exceptions_handler
    def ping_experiment(self, experiment):
        try:
            self.backend_swagger_client.api.pingExperiment(experimentId=experiment.internal_id).response()
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

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
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

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
                        SystemMetricPoint(
                            timestampMillis=int(metric_value.timestamp * 1000.0),
                            x=int(metric_value.running_time * 1000.0),
                            y=metric_value.value
                        )
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
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)

    @with_api_exceptions_handler
    def upload_experiment_output(self, experiment, data):
        with self._upload_loop(
                # pylint: disable=bad-continuation
                partial(self._upload_raw_data, api_method=self.backend_swagger_client.api.uploadExperimentOutput),
                data=data,
                path_params={'experimentId': experiment.internal_id},
                query_params={}
        ) as response:
            if response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
            elif (response.status_code == UNPROCESSABLE_ENTITY
                  and extract_response_field(response.content, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
            else:
                response.raise_for_status()

    @with_api_exceptions_handler
    def extract_experiment_output(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentOutputAsTarstream,
                data=data
            )
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project_full_id)
        except HTTPUnprocessableEntity as e:
            if extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED':
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
            parameter_type = 'double' if is_float(str(value)) and not isinstance(value, six.string_types) else 'string'

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

    def _convert_to_experiment(self, api_experiment):
        return Experiment(client=self,
                          _id=api_experiment.shortId,
                          internal_id=api_experiment.id,
                          project_full_id='{}/{}'.format(api_experiment.organizationName, api_experiment.projectName))

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

    def _upload_raw_data(self, api_method, data, headers, path_params, query_params):
        url = self.api_address + api_method.operation.path_name + "?"

        for key, val in path_params.items():
            url = url.replace("{" + key + "}", val)

        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

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


    def _update_proxies(self):
        try:
            self._http_client.session.proxies.update(self.proxies)
        except:
            # TODO: change error type and info
            raise ValueError("Error when using proxies {}".format(self.proxies))


uuid_format = SwaggerFormat(
    format='uuid',
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description=''
)
