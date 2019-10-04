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
import io
import logging
import os
import platform
import uuid
from functools import partial
from http.client import NOT_FOUND, UNPROCESSABLE_ENTITY  # pylint:disable=no-name-in-module
from io import StringIO
from itertools import groupby

import requests
import six
import urllib3
from bravado.client import SwaggerClient
from bravado.exception import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity, HTTPConflict
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat
from requests.exceptions import HTTPError

from neptune.api_exceptions import ExperimentAlreadyFinished, ExperimentLimitReached, \
    ExperimentNotFound, ExperimentValidationError, NamespaceNotFound, ProjectNotFound, StorageLimitReached, \
    ChannelAlreadyExists, ChannelsValuesSendBatchError, NotebookNotFound, \
    PathInProjectNotFound, ChannelNotFound
from neptune.backend import Backend
from neptune.checkpoint import Checkpoint
from neptune.exceptions import FileNotFound
from neptune.experiments import Experiment
from neptune.internal.backends.credentials import Credentials
from neptune.internal.utils.http import extract_response_field
from neptune.model import ChannelWithLastValue, LeaderboardEntry
from neptune.notebook import Notebook
from neptune.oauth import NeptuneAuthenticator
from neptune.projects import Project
from neptune.utils import is_float, with_api_exceptions_handler

_logger = logging.getLogger(__name__)


class HostedNeptuneBackend(Backend):

    @with_api_exceptions_handler
    def __init__(self, api_token=None, proxies=None):
        self.credentials = Credentials(api_token)

        ssl_verify = True
        if os.getenv("NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = RequestsClient(ssl_verify=ssl_verify)
        if proxies is not None:
            self._update_proxies(proxies)

        self.backend_swagger_client = self._get_swagger_client('{}/api/backend/swagger.json'
                                                               .format(self.api_address))

        self.leaderboard_swagger_client = self._get_swagger_client('{}/api/leaderboard/swagger.json'
                                                                   .format(self.api_address))

        self.authenticator = self._create_authenticator(self.credentials.api_token, ssl_verify)
        self._http_client.authenticator = self.authenticator

        # This is not a top-level import because of circular dependencies
        from neptune import __version__
        self.client_lib_version = __version__

        user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
            lib_version=self.client_lib_version,
            system=platform.platform(),
            python_version=platform.python_version())
        self._http_client.session.headers.update({'User-Agent': user_agent})

    @property
    def api_address(self):
        return self.credentials.api_address

    @with_api_exceptions_handler
    def get_project(self, project_qualified_name):
        try:
            project = self.backend_swagger_client.api.getProject(
                projectIdentifier=project_qualified_name
            ).response().result

            return Project(
                backend=self,
                internal_id=project.id,
                namespace=project.organizationName,
                name=project.name)
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
                    tagsMode='and', minRunningTimeSeconds=min_running_time,
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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                          hostname,
                          notebook_id,
                          checkpoint_id):
        if not isinstance(name, six.string_types):
            raise ValueError("Invalid name {}, should be a string.".format(name))
        if not isinstance(description, six.string_types):
            raise ValueError("Invalid description {}, should be a string.".format(description))
        if not isinstance(params, dict):
            raise ValueError("Invalid params {}, should be a dict.".format(params))
        if not isinstance(properties, dict):
            raise ValueError("Invalid properties {}, should be a dict.".format(properties))
        if not isinstance(hostname, six.string_types):
            raise ValueError("Invalid hostname {}, should be a string.".format(hostname))

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
                hostname=hostname,
                notebookId=notebook_id,
                checkpointId=checkpoint_id
            )

            kwargs = {
                'experimentCreationParams': params,
                'X-Neptune-CliVersion': self.client_lib_version
            }
            api_experiment = self.backend_swagger_client.api.createExperiment(**kwargs).response().result

            return self._convert_to_experiment(api_experiment, project)
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
                backend=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner
            )
        except HTTPNotFound:
            raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

    @with_api_exceptions_handler
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
                backend=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id, jupyter_path, _file=None):
        if _file is not None:
            with self._upload_raw_data(api_method=self.leaderboard_swagger_client.api.createCheckpoint,
                                       data=_file,
                                       headers={"Content-Type": "application/octet-stream"},
                                       path_params={
                                           "notebookId": notebook_id
                                       },
                                       query_params={
                                           "jupyterPath": jupyter_path
                                       }) as response:
                if response.status_code == NOT_FOUND:
                    raise NotebookNotFound(notebook_id=notebook_id)
                else:
                    response.raise_for_status()
                    CheckpointDTO = self.leaderboard_swagger_client.get_model('CheckpointDTO')
                    return CheckpointDTO.unmarshal(response.json())
        else:
            NewCheckpointDTO = self.leaderboard_swagger_client.get_model('NewCheckpointDTO')
            return self.leaderboard_swagger_client.api.createEmptyCheckpoint(
                notebookId=notebook_id,
                checkpoint=NewCheckpointDTO(path=jupyter_path)
            ).response().result

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
                project_qualified_name=experiment._project.full_id
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
                project_qualified_name=experiment._project.full_id
            )
        except HTTPBadRequest as e:
            error_type = extract_response_field(e.response, 'type')
            if error_type == 'INVALID_TAG':
                raise ExperimentValidationError(extract_response_field(e.response, 'message'))
            else:
                raise

    def upload_experiment_source(self, experiment, data):
        try:
            # Api exception handling is done in _upload_loop
            self._upload_loop(partial(self._upload_raw_data,
                                      api_method=self.backend_swagger_client.api.uploadExperimentSource),
                              data=data,
                              path_params={'experimentId': experiment.internal_id},
                              query_params={})
        except HTTPError as e:
            if e.response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
            if e.response.status_code == UNPROCESSABLE_ENTITY and (
                    extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
            raise

    def extract_experiment_source(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentSourceAsTarstream,
                data=data
            )
        except HTTPError as e:
            if e.response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
            if e.response.status_code == UNPROCESSABLE_ENTITY and (
                    extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
        except HTTPConflict:
            raise ChannelAlreadyExists(channel_name=name, experiment_short_id=experiment.id)

    @with_api_exceptions_handler
    def reset_channel(self, channel_id):

        try:
            self.backend_swagger_client.api.resetChannel(
                id=channel_id
            ).response()
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ChannelNotFound(channel_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
        except HTTPUnprocessableEntity:
            raise ExperimentAlreadyFinished(experiment.id)

    @with_api_exceptions_handler
    def ping_experiment(self, experiment):
        try:
            self.backend_swagger_client.api.pingExperiment(experimentId=experiment.internal_id).response()
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

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
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

    def upload_experiment_output(self, experiment, data):
        try:
            # Api exception handling is done in _upload_loop
            self._upload_loop(partial(self._upload_raw_data,
                                      api_method=self.backend_swagger_client.api.uploadExperimentOutput),
                              data=data,
                              path_params={'experimentId': experiment.internal_id},
                              query_params={})
        except HTTPError as e:
            if e.response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
            if e.response.status_code == UNPROCESSABLE_ENTITY and (
                    extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
            raise

    def extract_experiment_output(self, experiment, data):
        try:
            return self._upload_tar_data(
                experiment=experiment,
                api_method=self.backend_swagger_client.api.uploadExperimentOutputAsTarstream,
                data=data
            )
        except HTTPError as e:
            if e.response.status_code == NOT_FOUND:
                # pylint: disable=protected-access
                raise ExperimentNotFound(
                    experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
            if e.response.status_code == UNPROCESSABLE_ENTITY and (
                    extract_response_field(e.response, 'type') == 'LIMIT_OF_STORAGE_IN_PROJECT_REACHED'):
                raise StorageLimitReached()
            raise

    @with_api_exceptions_handler
    def download_data(self, project, path, destination):
        with self._download_raw_data(api_method=self.backend_swagger_client.api.downloadData,
                                     headers={"Accept": "application/octet-stream"},
                                     path_params={},
                                     query_params={
                                         "projectId": project.internal_id,
                                         "path": path
                                     }) as response:
            if response.status_code == NOT_FOUND:
                raise PathInProjectNotFound(path=path, project_identifier=project.full_id)
            else:
                response.raise_for_status()

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)

    @with_api_exceptions_handler
    def prepare_source_download_reuqest(self, experiment, path):
        try:
            return self.backend_swagger_client.api.prepareForDownload(
                experimentIdentity=experiment.internal_id,
                resource='source',
                path=path
            ).response().result
        except HTTPNotFound:
            raise FileNotFound(path)

    @with_api_exceptions_handler
    def prepare_output_download_reuqest(self, experiment, path):
        try:
            return self.backend_swagger_client.api.prepareForDownload(
                experimentIdentity=experiment.internal_id,
                resource='output',
                path=path
            ).response().result
        except HTTPNotFound:
            raise FileNotFound(path)

    @with_api_exceptions_handler
    def get_download_request(self, request_id):
        return self.backend_swagger_client.api.getDownloadRequest(id=request_id).response().result

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

    def _convert_to_experiment(self, api_experiment, project):
        return Experiment(backend=self,
                          project=project,
                          _id=api_experiment.shortId,
                          internal_id=api_experiment.id)

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
                part_to_send = part.get_data()
                ret = with_api_exceptions_handler(self._upload_loop_chunk)(fun, part, part_to_send, data, **kwargs)
            else:
                part.skip()
        data.close()
        return ret

    def _upload_loop_chunk(self, fun, part, part_to_send, data, **kwargs):
        if part.end:
            binary_range = "bytes=%d-%d/%d" % (part.start, part.end - 1, data.length)
        else:
            binary_range = "bytes=%d-/%d" % (part.start, data.length)
        response = fun(data=part_to_send,
                       headers={
                           "Content-Type": "application/octet-stream",
                           "Content-Filename": data.filename,
                           "Range": binary_range,
                           "X-File-Permissions": data.permissions
                       },
                       **kwargs)
        response.raise_for_status()
        return response

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

    def _download_raw_data(self, api_method, headers, path_params, query_params):
        url = self.api_address + api_method.operation.path_name + "?"

        for key, val in path_params.items():
            url = url.replace("{" + key + "}", val)

        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

        session = self._http_client.session

        request = self.authenticator.apply(
            requests.Request(
                method='GET',
                url=url,
                headers=headers
            )
        )

        return session.send(session.prepare_request(request), stream=True)

    @with_api_exceptions_handler
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

        response = session.send(session.prepare_request(request))
        response.raise_for_status()
        return response

    def _update_proxies(self, proxies):
        try:
            self._http_client.session.proxies.update(proxies)
        except (TypeError, ValueError):
            raise ValueError("Wrong proxies format: {}".format(proxies))

    @with_api_exceptions_handler
    def _get_swagger_client(self, url):
        return SwaggerClient.from_url(
            url,
            config=dict(
                validate_swagger_spec=False,
                validate_requests=False,
                validate_responses=False,
                formats=[uuid_format]
            ),
            http_client=self._http_client
        )

    @with_api_exceptions_handler
    def _create_authenticator(self, api_token, ssl_verify):
        return NeptuneAuthenticator(
            self.backend_swagger_client.api.exchangeApiToken(X_Neptune_Api_Token=api_token).response().result,
            ssl_verify
        )


uuid_format = SwaggerFormat(
    format='uuid',
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description=''
)
