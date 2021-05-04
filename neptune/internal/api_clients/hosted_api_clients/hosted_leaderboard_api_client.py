#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

# pylint: disable=too-many-lines

import io
import json
import logging
import math
import os
import re
import sys
import uuid
from datetime import time
from functools import partial
from http.client import NOT_FOUND  # pylint:disable=no-name-in-module
from io import StringIO
from itertools import groupby
from typing import Dict

import requests
import six
from bravado.exception import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity, HTTPConflict
from neptune.internal.websockets.reconnecting_websocket_factory import ReconnectingWebsocketFactory

from neptune.api_exceptions import (
    ChannelAlreadyExists,
    ChannelNotFound,
    ChannelsValuesSendBatchError,
    ExperimentAlreadyFinished,
    ExperimentLimitReached,
    ExperimentNotFound,
    ExperimentValidationError,
    NotebookNotFound,
    PathInProjectNotFound,
    ProjectNotFound,
)
from neptune.backend import LeaderboardApiClient
from neptune.checkpoint import Checkpoint
from neptune.exceptions import FileNotFound
from neptune.experiments import Experiment
from neptune.internal.api_clients.hosted_api_clients.mixins import HostedNeptuneMixin
from neptune.internal.storage.storage_utils import UploadEntry, normalize_file_name, upload_to_storage
from neptune.internal.utils.http_utils import extract_response_field, handle_quota_limits
from neptune.model import ChannelWithLastValue, LeaderboardEntry
from neptune.notebook import Notebook
from neptune.utils import NoopObject, assure_directory_exists, with_api_exceptions_handler

_logger = logging.getLogger(__name__)


class HostedNeptuneLeaderboardApiClient(HostedNeptuneMixin, LeaderboardApiClient):
    @with_api_exceptions_handler
    def __init__(self, backend_api_client):
        self._backend_api_client = backend_api_client

        self._client_config = self._create_client_config(api_token=self.credentials.api_token,
                                                         backend_client=self.backend_client)

        self.leaderboard_swagger_client = self._get_swagger_client(
            '{}/api/leaderboard/swagger.json'.format(self._client_config.api_url),
            self._backend_api_client.http_client
        )

        if sys.version_info >= (3, 7):
            try:
                # pylint: disable=no-member
                os.register_at_fork(after_in_child=self._handle_fork_in_child)
            except AttributeError:
                pass

    def _handle_fork_in_child(self):
        self.leaderboard_swagger_client = NoopObject()

    @property
    def http_client(self):
        return self._backend_api_client.http_client

    @property
    def backend_client(self):
        return self._backend_api_client.backend_client

    @property
    def authenticator(self):
        return self._backend_api_client.authenticator

    @property
    def credentials(self):
        return self._backend_api_client.credentials

    @property
    def backend_swagger_client(self):
        return self._backend_api_client.backend_swagger_client

    @property
    def client_lib_version(self):
        return self._backend_api_client.client_lib_version

    @property
    def api_address(self):
        return self._client_config.api_url

    @property
    def display_address(self):
        return self._backend_api_client.display_address

    @property
    def proxies(self):
        return self._backend_api_client.proxies

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

    def websockets_factory(self, project_uuid, experiment_id):
        base_url = re.sub(r'^http', 'ws', self.api_address) + '/api/notifications/v1'
        return ReconnectingWebsocketFactory(
            backend=self,
            url=base_url + '/experiments/' + experiment_id + '/operations'
        )

    @with_api_exceptions_handler
    def get_channel_points_csv(self, experiment, channel_internal_id, channel_name):
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
                          entrypoint,
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
        if entrypoint is not None and not isinstance(entrypoint, six.string_types):
            raise ValueError("Invalid entrypoint {}, should be a string.".format(entrypoint))

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
                remotes=git_info.remote_urls,
                currentBranch=git_info.active_branch,
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
                enqueueCommand="command",  # legacy (it's ignored but any non-empty string is required)
                entrypoint=entrypoint,
                execArgsTemplate="",  # legacy,
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

    def upload_source_code(self, experiment, source_target_pairs):
        upload_source_entries = [
            UploadEntry(source_path, target_path)
            for source_path, target_path in source_target_pairs
        ]
        upload_to_storage(upload_entries=upload_source_entries,
                          upload_api_fun=self.upload_experiment_source,
                          upload_tar_api_fun=self.extract_experiment_source,
                          warn_limit=100 * 1024 * 1024,
                          experiment=experiment)

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
            try:
                NewCheckpointDTO = self.leaderboard_swagger_client.get_model('NewCheckpointDTO')
                return self.leaderboard_swagger_client.api.createEmptyCheckpoint(
                    notebookId=notebook_id,
                    checkpoint=NewCheckpointDTO(path=jupyter_path)
                ).response().result
            except HTTPNotFound:
                return None

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
    def set_property(self, experiment, key, value):
        properties = {p.key: p.value for p in self.get_experiment(experiment.internal_id).properties}
        properties[key] = str(value)
        return self.update_experiment(
            experiment=experiment,
            properties=properties
        )

    @with_api_exceptions_handler
    def remove_property(self, experiment, key):
        properties = {p.key: p.value for p in self.get_experiment(experiment.internal_id).properties}
        del properties[key]
        return self.update_experiment(
            experiment=experiment,
            properties=properties
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

    @handle_quota_limits
    def upload_experiment_source(self, experiment, data, progress_indicator):
        self._upload_loop(partial(self._upload_raw_data,
                                  api_method=self.backend_swagger_client.api.uploadExperimentSource),
                          data=data,
                          progress_indicator=progress_indicator,
                          path_params={'experimentId': experiment.internal_id},
                          query_params={})

    @handle_quota_limits
    def extract_experiment_source(self, experiment, data):
        return self._upload_tar_data(
            experiment=experiment,
            api_method=self.backend_swagger_client.api.uploadExperimentSourceAsTarstream,
            data=data
        )

    @with_api_exceptions_handler
    def create_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
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
    def get_channels(self, experiment) -> Dict[str, object]:
        api_experiment = self.get_experiment(experiment.internal_id)
        channels_last_values_by_name = dict((ch.channelName, ch) for ch in api_experiment.channelsLastValues)
        channels = dict()
        for ch in api_experiment.channels:
            last_value = channels_last_values_by_name.get(ch.name, None)
            if last_value is not None:
                ch.x = last_value.x
                ch.y = last_value.y
            elif ch.lastX is not None:
                ch.x = ch.lastX
                ch.y = None
            else:
                ch.x = None
                ch.y = None
            channels[ch.name] = ch
        return channels

    @with_api_exceptions_handler
    def reset_channel(self, experiment, channel_id, channel_name, channel_type):
        try:
            self.backend_swagger_client.api.resetChannel(
                id=channel_id
            ).response()
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ChannelNotFound(channel_id)

    @with_api_exceptions_handler
    def create_system_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
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
    def get_system_channels(self, experiment) -> Dict[str, object]:
        try:
            channels = self.backend_swagger_client.api.getSystemChannels(
                experimentId=experiment.internal_id,
            ).response().result

            return {ch.name: ch for ch in channels}
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
        except HTTPNotFound as e:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id) from e
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
                name=metric.name, description=metric.description,
                resourceType=metric.resource_type.replace("MEMORY", "RAM"),
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

    def log_artifact(self, experiment, artifact, destination=None):
        if isinstance(artifact, str):
            if os.path.exists(artifact):
                target_name = os.path.basename(artifact) if destination is None else destination
                upload_entry = UploadEntry(os.path.abspath(artifact), normalize_file_name(target_name))
            else:
                raise FileNotFound(artifact)
        elif hasattr(artifact, 'read'):
            if destination is not None:
                upload_entry = UploadEntry(artifact, normalize_file_name(destination))
            else:
                raise ValueError("destination is required for file streams")
        else:
            raise ValueError("Artifact must be a local path or an IO object")

        upload_to_storage(upload_entries=[upload_entry],
                          upload_api_fun=self._upload_experiment_output,
                          upload_tar_api_fun=self._extract_experiment_output,
                          experiment=experiment)

    def delete_artifacts(self, experiment, path):
        if path is None:
            raise ValueError("path argument must not be None")

        paths = path
        if not isinstance(path, list):
            paths = [path]
        for path in paths:
            if path is None:
                raise ValueError("path argument must not be None")
            normalized_path = os.path.normpath(path)
            if normalized_path.startswith(".."):
                raise ValueError("path to delete must be within project's directory")
            if normalized_path == "." or normalized_path == "/" or not normalized_path:
                raise ValueError("Cannot delete whole artifacts directory")
        try:
            for path in paths:
                self.rm_data(experiment=experiment, path=path)
        except PathInProjectNotFound:
            raise FileNotFound(path)

    @with_api_exceptions_handler
    def rm_data(self, experiment, path):
        try:
            return self.backend_swagger_client.api.deleteExperimentOutput(
                experimentIdentifier=str(experiment.internal_id),
                path=path).response().result
        except BaseException as e:
            print("exception was raised: ", e)
            raise e

    @with_api_exceptions_handler
    def download_data(self, experiment, path, destination):
        project_storage_path = "/{exp_id}/output/{file}".format(exp_id=experiment.id, file=path)
        project = experiment._project  # pylint: disable=protected-access
        with self._download_raw_data(api_method=self.backend_swagger_client.api.downloadData,
                                     headers={"Accept": "application/octet-stream"},
                                     path_params={},
                                     query_params={
                                         "projectId": project.internal_id,
                                         "path": project_storage_path
                                     }) as response:
            if response.status_code == NOT_FOUND:
                raise PathInProjectNotFound(path=path, project_identifier=project.full_id)
            else:
                response.raise_for_status()

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)

    def download_sources(self, experiment, path=None, destination_dir=None):
        if not path:
            path = ""
        destination_dir = assure_directory_exists(destination_dir)

        download_request = self.prepare_source_download_request(experiment, path)
        self.download_from_request(download_request, destination_dir, path)

    @with_api_exceptions_handler
    def prepare_source_download_request(self, experiment, path):
        try:
            return self.backend_swagger_client.api.prepareForDownload(
                experimentIdentity=experiment.internal_id,
                resource='source',
                path=path
            ).response().result
        except HTTPNotFound:
            raise FileNotFound(path)

    def download_artifacts(self, experiment: Experiment, path=None, destination_dir=None):
        if not path:
            path = ""
        destination_dir = assure_directory_exists(destination_dir)

        download_request = self.prepare_output_download_request(experiment, path)
        self.download_from_request(download_request, destination_dir, path)

    def download_artifact(self, experiment: Experiment, path=None, destination_dir=None):
        destination_dir = assure_directory_exists(destination_dir)
        destination_path = os.path.join(destination_dir, os.path.basename(path))

        try:
            self.download_data(experiment, path, destination_path)
        except PathInProjectNotFound as e:
            raise FileNotFound(path) from e

    @with_api_exceptions_handler
    def prepare_output_download_request(self, experiment, path):
        try:
            return self.backend_swagger_client.api.prepareForDownload(
                experimentIdentity=experiment.internal_id,
                resource='output',
                path=path
            ).response().result
        except HTTPNotFound:
            raise FileNotFound(path)

    def download_from_request(self, download_request, destination_dir, path):
        sleep_time = 1
        max_sleep_time = 16
        while not hasattr(download_request, "downloadUrl"):
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep_time)
            download_request = self.get_download_request(download_request.id)

        ssl_verify = True
        if os.getenv("NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"):
            ssl_verify = False

        # We do not use ApiClient here cause `downloadUrl` can be any url (not only Neptune API endpoint)
        response = requests.get(
            url=download_request.downloadUrl,
            headers={"Accept": "application/zip"},
            stream=True,
            verify=ssl_verify
        )

        with response:
            filename = None
            if 'content-disposition' in response.headers:
                content_disposition = response.headers['content-disposition']
                filenames = re.findall("filename=(.+)", content_disposition)
                if filenames:
                    filename = filenames[0]

            if not filename:
                filename = os.path.basename(path.rstrip("/")) + ".zip"

            destination_path = os.path.join(destination_dir, filename)
            with open(destination_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)

    @with_api_exceptions_handler
    def get_download_request(self, request_id):
        return self.backend_swagger_client.api.getDownloadRequest(id=request_id).response().result

    @handle_quota_limits
    def _upload_experiment_output(self, experiment, data, progress_indicator):
        self._upload_loop(partial(self._upload_raw_data,
                                  api_method=self.backend_swagger_client.api.uploadExperimentOutput),
                          data=data,
                          progress_indicator=progress_indicator,
                          path_params={'experimentId': experiment.internal_id},
                          query_params={})

    @handle_quota_limits
    def _extract_experiment_output(self, experiment, data):
        return self._upload_tar_data(
            experiment=experiment,
            api_method=self.backend_swagger_client.api.uploadExperimentOutputAsTarstream,
            data=data
        )

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []

        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items

        return items

    def _get_parameter_with_type(self, parameter):
        string_type = 'string'
        double_type = 'double'
        if isinstance(parameter, bool):
            return (string_type, str(parameter))
        elif isinstance(parameter, float) or isinstance(parameter, int):
            if math.isinf(parameter) or math.isnan(parameter):
                return (string_type, json.dumps(parameter))
            else:
                return (double_type, str(parameter))
        else:
            return (string_type, str(parameter))

    def _convert_to_api_parameters(self, raw_params):
        Parameter = self.backend_swagger_client.get_model('Parameter')

        params = []
        for name, value in raw_params.items():
            (parameter_type, string_value) = self._get_parameter_with_type(value)
            params.append(
                Parameter(
                    id=str(uuid.uuid4()),
                    name=name,
                    parameterType=parameter_type,
                    value=string_value
                )
            )

        return params

    def _convert_to_api_properties(self, raw_properties):
        KeyValueProperty = self.backend_swagger_client.get_model('KeyValueProperty')

        return [
            KeyValueProperty(
                key=key,
                value=str(value)
            ) for key, value in raw_properties.items()
        ]

    def _convert_to_experiment(self, api_experiment, project):
        # pylint: disable=protected-access
        return Experiment(backend=project._backend,
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

    def _upload_loop(self, fun, data, progress_indicator, **kwargs):
        ret = None
        for part in data.generate():
            ret = with_api_exceptions_handler(self._upload_loop_chunk)(fun, part, data, **kwargs)
            progress_indicator.progress(part.end - part.start)

        data.close()
        return ret

    def _upload_loop_chunk(self, fun, part, data, **kwargs):
        if data.length is not None:
            binary_range = "bytes=%d-%d/%d" % (part.start, part.end - 1, data.length)
        else:
            binary_range = "bytes=%d-%d" % (part.start, part.end - 1)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Filename": data.filename,
            "X-Range": binary_range,
        }
        if data.permissions is not None:
            headers["X-File-Permissions"] = data.permissions
        response = fun(data=part.get_data(), headers=headers, **kwargs)
        response.raise_for_status()
        return response

    def _upload_raw_data(self, api_method, data, headers, path_params, query_params):
        url = self.api_address + api_method.operation.path_name + "?"

        for key, val in path_params.items():
            url = url.replace("{" + key + "}", val)

        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

        session = self.http_client.session

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

        session = self.http_client.session

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

        session = self.http_client.session

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
