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

import json
import logging
import math
import os
import re
import sys
import time
from collections import namedtuple
from http.client import NOT_FOUND
from io import StringIO
from itertools import groupby
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
)

import requests
import six
from bravado.exception import HTTPNotFound

from neptune.common import exceptions as common_exceptions
from neptune.common.exceptions import ClientHttpError
from neptune.common.experiments import Experiment
from neptune.common.storage.storage_utils import normalize_file_name
from neptune.common.utils import (
    NoopObject,
    assure_directory_exists,
)
from neptune.legacy.api_exceptions import (
    ExperimentNotFound,
    ExperimentOperationErrors,
    NotebookNotFound,
    PathInExperimentNotFound,
    ProjectNotFound,
)
from neptune.legacy.backend import LeaderboardApiClient
from neptune.legacy.checkpoint import Checkpoint
from neptune.legacy.exceptions import (
    DeleteArtifactUnsupportedInAlphaException,
    DownloadArtifactsUnsupportedException,
    DownloadArtifactUnsupportedException,
    DownloadSourcesException,
    FileNotFound,
    NeptuneException,
)
from neptune.legacy.internal.api_clients.hosted_api_clients.mixins import HostedNeptuneMixin
from neptune.legacy.internal.api_clients.hosted_api_clients.utils import legacy_with_api_exceptions_handler
from neptune.legacy.internal.channels.channels import (
    ChannelNamespace,
    ChannelType,
    ChannelValueType,
)
from neptune.legacy.internal.utils.alpha_integration import (
    AlphaChannelDTO,
    AlphaChannelWithValueDTO,
    AlphaParameterDTO,
    AlphaPropertyDTO,
    channel_type_to_clear_operation,
    channel_type_to_operation,
    channel_value_type_to_operation,
    deprecated_img_to_alpha_image,
)
from neptune.legacy.internal.websockets.reconnecting_websocket_factory import ReconnectingWebsocketFactory
from neptune.legacy.model import (
    ChannelWithLastValue,
    LeaderboardEntry,
)
from neptune.legacy.notebook import Notebook
from neptune.new import exceptions as alpha_exceptions
from neptune.new.attributes import constants as alpha_consts
from neptune.new.attributes.constants import (
    MONITORING_TRACEBACK_ATTRIBUTE_PATH,
    SYSTEM_FAILED_ATTRIBUTE_PATH,
)
from neptune.new.internal import operation as alpha_operation
from neptune.new.internal.backends import hosted_file_operations as alpha_hosted_file_operations
from neptune.new.internal.backends.api_model import AttributeType
from neptune.new.internal.backends.operation_api_name_visitor import (
    OperationApiNameVisitor as AlphaOperationApiNameVisitor,
)
from neptune.new.internal.backends.operation_api_object_converter import (
    OperationApiObjectConverter as AlphaOperationApiObjectConverter,
)
from neptune.new.internal.backends.utils import handle_server_raw_response_messages
from neptune.new.internal.operation import (
    AssignBool,
    AssignString,
    ConfigFloatSeries,
    LogFloats,
    LogStrings,
)
from neptune.new.internal.utils import (
    base64_decode,
    base64_encode,
)
from neptune.new.internal.utils import paths as alpha_path_utils
from neptune.new.internal.utils.paths import parse_path

_logger = logging.getLogger(__name__)

LegacyExperiment = namedtuple(
    "LegacyExperiment",
    "shortId "
    "name "
    "timeOfCreation "
    "timeOfCompletion "
    "runningTime "
    "owner "
    "storageSize "
    "channelsSize "
    "tags "
    "description "
    "hostname "
    "state "
    "properties "
    "parameters",
)

LegacyLeaderboardEntry = namedtuple(
    "LegacyExperiment",
    "id "
    "organizationName "
    "projectName "
    "shortId "
    "name "
    "state "
    "timeOfCreation "
    "timeOfCompletion "
    "runningTime "
    "owner "
    "size "
    "tags "
    "description "
    "channelsLastValues "
    "parameters "
    "properties",
)

if TYPE_CHECKING:
    from neptune.legacy.internal.api_clients import HostedNeptuneBackendApiClient


class HostedAlphaLeaderboardApiClient(HostedNeptuneMixin, LeaderboardApiClient):
    @legacy_with_api_exceptions_handler
    def __init__(self, backend_api_client: "HostedNeptuneBackendApiClient"):
        self._backend_api_client = backend_api_client

        self._client_config = self._create_client_config(
            api_token=self.credentials.api_token, backend_client=self.backend_client
        )

        self.leaderboard_swagger_client = self._get_swagger_client(
            "{}/api/leaderboard/swagger.json".format(self._client_config.api_url),
            self._backend_api_client.http_client,
        )

        if sys.version_info >= (3, 7):
            try:
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

    @legacy_with_api_exceptions_handler
    def get_project_members(self, project_identifier):
        try:
            r = self.backend_swagger_client.api.listProjectMembers(projectIdentifier=project_identifier).response()
            return r.result
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier)

    @legacy_with_api_exceptions_handler
    def create_experiment(
        self,
        project,
        name,
        description,
        params,
        properties,
        tags,
        abortable,  # deprecated in alpha
        monitored,  # deprecated in alpha
        git_info,
        hostname,
        entrypoint,
        notebook_id,
        checkpoint_id,
    ):
        if not isinstance(name, six.string_types):
            raise ValueError("Invalid name {}, should be a string.".format(name))
        if not isinstance(description, six.string_types):
            raise ValueError("Invalid description {}, should be a string.".format(description))
        if not isinstance(params, dict):
            raise ValueError("Invalid params {}, should be a dict.".format(params))
        if not isinstance(properties, dict):
            raise ValueError("Invalid properties {}, should be a dict.".format(properties))
        if hostname is not None and not isinstance(hostname, six.string_types):
            raise ValueError("Invalid hostname {}, should be a string.".format(hostname))
        if entrypoint is not None and not isinstance(entrypoint, six.string_types):
            raise ValueError("Invalid entrypoint {}, should be a string.".format(entrypoint))

        git_info = (
            {
                "commit": {
                    "commitId": git_info.commit_id,
                    "message": git_info.message,
                    "authorName": git_info.author_name,
                    "authorEmail": git_info.author_email,
                    "commitDate": git_info.commit_date,
                },
                "repositoryDirty": git_info.repository_dirty,
                "currentBranch": git_info.active_branch,
                "remotes": git_info.remote_urls,
            }
            if git_info
            else None
        )

        api_params = {
            "notebookId": notebook_id,
            "checkpointId": checkpoint_id,
            "projectIdentifier": str(project.internal_id),
            "cliVersion": self.client_lib_version,
            "gitInfo": git_info,
            "customId": None,
        }

        kwargs = {
            "experimentCreationParams": api_params,
            "X-Neptune-CliVersion": self.client_lib_version,
            "_request_options": {"headers": {"X-Neptune-LegacyClient": "true"}},
        }

        try:
            api_experiment = self.leaderboard_swagger_client.api.createExperiment(**kwargs).response().result
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

        experiment = self._convert_to_experiment(api_experiment, project)
        # Initialize new experiment
        init_experiment_operations = self._get_init_experiment_operations(
            name, description, params, properties, tags, hostname, entrypoint
        )
        self._execute_operations(
            experiment=experiment,
            operations=init_experiment_operations,
        )
        return experiment

    def upload_source_code(self, experiment, source_target_pairs):
        dest_path = alpha_path_utils.parse_path(alpha_consts.SOURCE_CODE_FILES_ATTRIBUTE_PATH)
        file_globs = [source_path for source_path, target_path in source_target_pairs]
        upload_files_operation = alpha_operation.UploadFileSet(
            path=dest_path,
            file_globs=file_globs,
            reset=True,
        )
        self._execute_upload_operations_with_400_retry(experiment, upload_files_operation)

    @legacy_with_api_exceptions_handler
    def get_notebook(self, project, notebook_id):
        try:
            api_notebook_list = (
                self.leaderboard_swagger_client.api.listNotebooks(
                    projectIdentifier=project.internal_id, id=[notebook_id]
                )
                .response()
                .result
            )

            if not api_notebook_list.entries:
                raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

            api_notebook = api_notebook_list.entries[0]

            return Notebook(
                backend=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner,
            )
        except HTTPNotFound:
            raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

    @legacy_with_api_exceptions_handler
    def get_last_checkpoint(self, project, notebook_id):
        try:
            api_checkpoint_list = (
                self.leaderboard_swagger_client.api.listCheckpoints(notebookId=notebook_id, offset=0, limit=1)
                .response()
                .result
            )

            if not api_checkpoint_list.entries:
                raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

            checkpoint = api_checkpoint_list.entries[0]
            return Checkpoint(checkpoint.id, checkpoint.name, checkpoint.path)
        except HTTPNotFound:
            raise NotebookNotFound(notebook_id=notebook_id, project=project.full_id)

    @legacy_with_api_exceptions_handler
    def create_notebook(self, project):
        try:
            api_notebook = (
                self.leaderboard_swagger_client.api.createNotebook(projectIdentifier=project.internal_id)
                .response()
                .result
            )

            return Notebook(
                backend=self,
                project=project,
                _id=api_notebook.id,
                owner=api_notebook.owner,
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

    @legacy_with_api_exceptions_handler
    def create_checkpoint(self, notebook_id, jupyter_path, _file=None):
        if _file is not None:
            with self._upload_raw_data(
                api_method=self.leaderboard_swagger_client.api.createCheckpoint,
                data=_file,
                headers={"Content-Type": "application/octet-stream"},
                path_params={"notebookId": notebook_id},
                query_params={"jupyterPath": jupyter_path},
            ) as response:
                if response.status_code == NOT_FOUND:
                    raise NotebookNotFound(notebook_id=notebook_id)
                else:
                    response.raise_for_status()
                    CheckpointDTO = self.leaderboard_swagger_client.get_model("CheckpointDTO")
                    return CheckpointDTO.unmarshal(response.json())
        else:
            try:
                NewCheckpointDTO = self.leaderboard_swagger_client.get_model("NewCheckpointDTO")
                return (
                    self.leaderboard_swagger_client.api.createEmptyCheckpoint(
                        notebookId=notebook_id,
                        checkpoint=NewCheckpointDTO(path=jupyter_path),
                    )
                    .response()
                    .result
                )
            except HTTPNotFound:
                return None

    @legacy_with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        api_attributes = self._get_api_experiment_attributes(experiment_id)
        attributes = api_attributes.attributes
        system_attributes = api_attributes.systemAttributes

        return LegacyExperiment(
            shortId=system_attributes.shortId.value,
            name=system_attributes.name.value,
            timeOfCreation=system_attributes.creationTime.value,
            timeOfCompletion=None,
            runningTime=system_attributes.runningTime.value,
            owner=system_attributes.owner.value,
            storageSize=system_attributes.size.value,
            channelsSize=0,
            tags=system_attributes.tags.values,
            description=system_attributes.description.value,
            hostname=system_attributes.hostname.value if system_attributes.hostname else None,
            state="running" if system_attributes.state.value == "running" else "succeeded",
            properties=[AlphaPropertyDTO(attr) for attr in attributes if AlphaPropertyDTO.is_valid_attribute(attr)],
            parameters=[AlphaParameterDTO(attr) for attr in attributes if AlphaParameterDTO.is_valid_attribute(attr)],
        )

    @legacy_with_api_exceptions_handler
    def set_property(self, experiment, key, value):
        """Save attribute casted to string under `alpha_consts.PROPERTIES_ATTRIBUTE_SPACE` namespace"""
        self._execute_operations(
            experiment=experiment,
            operations=[
                alpha_operation.AssignString(
                    path=alpha_path_utils.parse_path(f"{alpha_consts.PROPERTIES_ATTRIBUTE_SPACE}{key}"),
                    value=str(value),
                )
            ],
        )

    @legacy_with_api_exceptions_handler
    def remove_property(self, experiment, key):
        self._remove_attribute(experiment, str_path=f"{alpha_consts.PROPERTIES_ATTRIBUTE_SPACE}{key}")

    @legacy_with_api_exceptions_handler
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        operations = [
            alpha_operation.AddStrings(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
                values=tags_to_add,
            ),
            alpha_operation.RemoveStrings(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
                values=tags_to_delete,
            ),
        ]
        self._execute_operations(
            experiment=experiment,
            operations=operations,
        )

    @staticmethod
    def _get_channel_attribute_path(channel_name: str, channel_namespace: ChannelNamespace) -> str:
        if channel_namespace == ChannelNamespace.USER:
            prefix = alpha_consts.LOG_ATTRIBUTE_SPACE
        else:
            prefix = alpha_consts.MONITORING_ATTRIBUTE_SPACE
        return f"{prefix}{channel_name}"

    def _create_channel(
        self,
        experiment: Experiment,
        channel_id: str,
        channel_name: str,
        channel_type: ChannelType,
        channel_namespace: ChannelNamespace,
    ):
        """This function is responsible for creating 'fake' channels in alpha projects.

        Since channels are abandoned in alpha api, we're mocking them using empty logging operation."""

        operation = channel_type_to_operation(channel_type)

        log_empty_operation = operation(
            path=alpha_path_utils.parse_path(self._get_channel_attribute_path(channel_name, channel_namespace)),
            values=[],
        )  # this operation is used to create empty attribute
        self._execute_operations(
            experiment=experiment,
            operations=[log_empty_operation],
        )
        return ChannelWithLastValue(
            AlphaChannelWithValueDTO(
                channelId=channel_id,
                channelName=channel_name,
                channelType=channel_type.value,
                x=None,
                y=None,
            )
        )

    @legacy_with_api_exceptions_handler
    def create_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        channel_id = f"{alpha_consts.LOG_ATTRIBUTE_SPACE}{name}"
        return self._create_channel(
            experiment,
            channel_id,
            channel_name=name,
            channel_type=ChannelType(channel_type),
            channel_namespace=ChannelNamespace.USER,
        )

    def _get_channels(self, experiment) -> List[AlphaChannelDTO]:
        try:
            return [
                AlphaChannelDTO(attr)
                for attr in self._get_attributes(experiment.internal_id)
                if AlphaChannelDTO.is_valid_attribute(attr)
            ]
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project.full_id,
            )

    @legacy_with_api_exceptions_handler
    def get_channels(self, experiment) -> Dict[str, AlphaChannelDTO]:
        api_channels = [
            channel
            for channel in self._get_channels(experiment)
            # return channels from LOG_ATTRIBUTE_SPACE namespace only
            if channel.id.startswith(alpha_consts.LOG_ATTRIBUTE_SPACE)
        ]
        return {ch.name: ch for ch in api_channels}

    @legacy_with_api_exceptions_handler
    def create_system_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        channel_id = f"{alpha_consts.MONITORING_ATTRIBUTE_SPACE}{name}"
        return self._create_channel(
            experiment,
            channel_id,
            channel_name=name,
            channel_type=ChannelType(channel_type),
            channel_namespace=ChannelNamespace.SYSTEM,
        )

    @legacy_with_api_exceptions_handler
    def get_system_channels(self, experiment) -> Dict[str, AlphaChannelDTO]:
        return {
            channel.name: channel
            for channel in self._get_channels(experiment)
            if (
                channel.channelType == ChannelType.TEXT.value
                and channel.id.startswith(alpha_consts.MONITORING_ATTRIBUTE_SPACE)
            )
        }

    @legacy_with_api_exceptions_handler
    def send_channels_values(self, experiment, channels_with_values):
        send_operations = []
        for channel_with_values in channels_with_values:
            channel_value_type = channel_with_values.channel_type
            operation = channel_value_type_to_operation(channel_value_type)

            if channel_value_type == ChannelValueType.IMAGE_VALUE:
                # IMAGE_VALUE requires minor data modification before it's sent
                data_transformer = deprecated_img_to_alpha_image
            else:
                # otherwise use identity function as transformer
                data_transformer = lambda e: e  # noqa: E731

            ch_values = [
                alpha_operation.LogSeriesValue(
                    value=data_transformer(ch_value.value),
                    step=ch_value.x,
                    ts=ch_value.ts,
                )
                for ch_value in channel_with_values.channel_values
            ]
            send_operations.append(
                operation(
                    path=alpha_path_utils.parse_path(
                        self._get_channel_attribute_path(
                            channel_with_values.channel_name,
                            channel_with_values.channel_namespace,
                        )
                    ),
                    values=ch_values,
                )
            )

        self._execute_operations(experiment, send_operations)

    def mark_failed(self, experiment, traceback):
        operations = []
        path = parse_path(SYSTEM_FAILED_ATTRIBUTE_PATH)
        traceback_values = [LogStrings.ValueType(val, step=None, ts=time.time()) for val in traceback.split("\n")]
        operations.append(AssignBool(path=path, value=True))
        operations.append(
            LogStrings(
                values=traceback_values,
                path=parse_path(MONITORING_TRACEBACK_ATTRIBUTE_PATH),
            )
        )
        self._execute_operations(experiment, operations)

    def ping_experiment(self, experiment):
        try:
            self.leaderboard_swagger_client.api.ping(experimentId=str(experiment.internal_id)).response().result
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project.full_id,
            )

    @staticmethod
    def _get_attribute_name_for_metric(resource_type, gauge_name, gauges_count) -> str:
        if gauges_count > 1:
            return "monitoring/{}_{}".format(resource_type, gauge_name).lower()
        return "monitoring/{}".format(resource_type).lower()

    @legacy_with_api_exceptions_handler
    def create_hardware_metric(self, experiment, metric):
        operations = []
        gauges_count = len(metric.gauges)
        for gauge in metric.gauges:
            path = parse_path(self._get_attribute_name_for_metric(metric.resource_type, gauge.name(), gauges_count))
            operations.append(ConfigFloatSeries(path, min=metric.min_value, max=metric.max_value, unit=metric.unit))
        self._execute_operations(experiment, operations)

    @legacy_with_api_exceptions_handler
    def send_hardware_metric_reports(self, experiment, metrics, metric_reports):
        operations = []
        metrics_by_name = {metric.name: metric for metric in metrics}
        for report in metric_reports:
            metric = metrics_by_name.get(report.metric.name)
            gauges_count = len(metric.gauges)
            for gauge_name, metric_values in groupby(report.values, lambda value: value.gauge_name):
                metric_values = list(metric_values)
                path = parse_path(self._get_attribute_name_for_metric(metric.resource_type, gauge_name, gauges_count))
                operations.append(
                    LogFloats(
                        path,
                        [LogFloats.ValueType(value.value, step=None, ts=value.timestamp) for value in metric_values],
                    )
                )
        self._execute_operations(experiment, operations)

    def log_artifact(self, experiment, artifact, destination=None):
        if isinstance(artifact, str):
            if os.path.isfile(artifact):
                target_name = os.path.basename(artifact) if destination is None else destination
                dest_path = self._get_dest_and_ext(target_name)
                operation = alpha_operation.UploadFile(
                    path=dest_path,
                    ext="",
                    file_path=os.path.abspath(artifact),
                )
            elif os.path.isdir(artifact):
                for path, file_destination in self._log_dir_artifacts(artifact, destination):
                    self.log_artifact(experiment, path, file_destination)
                return
            else:
                raise FileNotFound(artifact)
        elif hasattr(artifact, "read"):
            if not destination:
                raise ValueError("destination is required for IO streams")
            dest_path = self._get_dest_and_ext(destination)
            data = artifact.read()
            content = data.encode("utf-8") if isinstance(data, str) else data
            operation = alpha_operation.UploadFileContent(path=dest_path, ext="", file_content=base64_encode(content))
        else:
            raise ValueError("Artifact must be a local path or an IO object")

        self._execute_upload_operations_with_400_retry(experiment, operation)

    @staticmethod
    def _get_dest_and_ext(target_name):
        qualified_target_name = f"{alpha_consts.ARTIFACT_ATTRIBUTE_SPACE}{target_name}"
        return alpha_path_utils.parse_path(normalize_file_name(qualified_target_name))

    def _log_dir_artifacts(self, directory_path, destination):
        directory_path = Path(directory_path)
        prefix = directory_path.name if destination is None else destination
        for path in directory_path.glob("**/*"):
            if path.is_file():
                relative_path = path.relative_to(directory_path)
                file_destination = prefix + "/" + str(relative_path)
                yield str(path), file_destination

    def delete_artifacts(self, experiment, path):
        try:
            self._remove_attribute(experiment, str_path=f"{alpha_consts.ARTIFACT_ATTRIBUTE_SPACE}{path}")
        except ExperimentOperationErrors as e:
            if all(isinstance(err, alpha_exceptions.MetadataInconsistency) for err in e.errors):
                raise DeleteArtifactUnsupportedInAlphaException(path, experiment) from None
            raise

    @legacy_with_api_exceptions_handler
    def download_data(self, experiment: Experiment, path: str, destination):
        project_storage_path = f"artifacts/{path}"
        with self._download_raw_data(
            api_method=self.leaderboard_swagger_client.api.downloadAttribute,
            headers={"Accept": "application/octet-stream"},
            path_params={},
            query_params={
                "experimentId": experiment.internal_id,
                "attribute": project_storage_path,
            },
        ) as response:
            if response.status_code == NOT_FOUND:
                raise PathInExperimentNotFound(path=path, exp_identifier=experiment.internal_id)
            else:
                response.raise_for_status()

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)

    def download_sources(self, experiment: Experiment, path=None, destination_dir=None):
        if path is not None:
            # in alpha all source files stored as single FileSet must be downloaded at once
            raise DownloadSourcesException(experiment)
        path = alpha_consts.SOURCE_CODE_FILES_ATTRIBUTE_PATH

        destination_dir = assure_directory_exists(destination_dir)

        download_request = self._get_file_set_download_request(experiment.internal_id, path)
        alpha_hosted_file_operations.download_file_set_attribute(
            swagger_client=self.leaderboard_swagger_client,
            download_id=download_request.id,
            destination=destination_dir,
        )

    @legacy_with_api_exceptions_handler
    def _get_file_set_download_request(self, run_id: str, path: str):
        params = {
            "experimentId": run_id,
            "attribute": path,
        }
        return self.leaderboard_swagger_client.api.prepareForDownloadFileSetAttributeZip(**params).response().result

    def download_artifacts(self, experiment: Experiment, path=None, destination_dir=None):
        raise DownloadArtifactsUnsupportedException(experiment)

    def download_artifact(self, experiment: Experiment, path=None, destination_dir=None):
        destination_dir = assure_directory_exists(destination_dir)
        destination_path = os.path.join(destination_dir, os.path.basename(path))

        try:
            self.download_data(experiment, path, destination_path)
        except PathInExperimentNotFound:
            raise DownloadArtifactUnsupportedException(path, experiment) from None

    def _get_attributes(self, experiment_id) -> list:
        return self._get_api_experiment_attributes(experiment_id).attributes

    def _get_api_experiment_attributes(self, experiment_id):
        params = {
            "experimentId": experiment_id,
        }
        return self.leaderboard_swagger_client.api.getExperimentAttributes(**params).response().result

    def _remove_attribute(self, experiment, str_path: str):
        """Removes given attribute"""
        self._execute_operations(
            experiment=experiment,
            operations=[
                alpha_operation.DeleteAttribute(
                    path=alpha_path_utils.parse_path(str_path),
                )
            ],
        )

    @staticmethod
    def _get_client_config_args(api_token):
        return dict(
            X_Neptune_Api_Token=api_token,
            alpha="true",
        )

    def _execute_upload_operation(self, experiment: Experiment, upload_operation: alpha_operation.Operation):
        experiment_id = experiment.internal_id
        try:
            if isinstance(upload_operation, alpha_operation.UploadFile):
                alpha_hosted_file_operations.upload_file_attribute(
                    swagger_client=self.leaderboard_swagger_client,
                    container_id=experiment_id,
                    attribute=alpha_path_utils.path_to_str(upload_operation.path),
                    source=upload_operation.file_path,
                    ext=upload_operation.ext,
                    multipart_config=self._client_config.multipart_config,
                )
            elif isinstance(upload_operation, alpha_operation.UploadFileContent):
                alpha_hosted_file_operations.upload_file_attribute(
                    swagger_client=self.leaderboard_swagger_client,
                    container_id=experiment_id,
                    attribute=alpha_path_utils.path_to_str(upload_operation.path),
                    source=base64_decode(upload_operation.file_content),
                    ext=upload_operation.ext,
                    multipart_config=self._client_config.multipart_config,
                )
            elif isinstance(upload_operation, alpha_operation.UploadFileSet):
                alpha_hosted_file_operations.upload_file_set_attribute(
                    swagger_client=self.leaderboard_swagger_client,
                    container_id=experiment_id,
                    attribute=alpha_path_utils.path_to_str(upload_operation.path),
                    file_globs=upload_operation.file_globs,
                    reset=upload_operation.reset,
                    multipart_config=self._client_config.multipart_config,
                )
            else:
                raise NeptuneException("Upload operation in neither File or FileSet")
        except common_exceptions.NeptuneException as e:
            raise NeptuneException(e) from e

        return None

    def _execute_upload_operations_with_400_retry(
        self, experiment: Experiment, upload_operation: alpha_operation.Operation
    ):
        while True:
            try:
                return self._execute_upload_operation(experiment, upload_operation)
            except ClientHttpError as ex:
                if "Length of stream does not match given range" not in ex.response:
                    raise ex

    @legacy_with_api_exceptions_handler
    def _execute_operations(self, experiment: Experiment, operations: List[alpha_operation.Operation]):
        experiment_id = experiment.internal_id
        file_operations = (
            alpha_operation.UploadFile,
            alpha_operation.UploadFileContent,
            alpha_operation.UploadFileSet,
        )
        if any(isinstance(op, file_operations) for op in operations):
            raise NeptuneException(
                "File operations must be handled directly by `_execute_upload_operation`,"
                " not by `_execute_operations` function call."
            )

        kwargs = {
            "experimentId": experiment_id,
            "operations": [
                {
                    "path": alpha_path_utils.path_to_str(op.path),
                    AlphaOperationApiNameVisitor().visit(op): AlphaOperationApiObjectConverter().convert(op),
                }
                for op in operations
            ],
        }
        try:
            result = self.leaderboard_swagger_client.api.executeOperations(**kwargs).response().result
            errors = [alpha_exceptions.MetadataInconsistency(err.errorDescription) for err in result]
            if errors:
                raise ExperimentOperationErrors(errors=errors)
            return None
        except HTTPNotFound as e:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project.full_id,
            ) from e

    def _get_init_experiment_operations(
        self, name, description, params, properties, tags, hostname, entrypoint
    ) -> List[alpha_operation.Operation]:
        """Returns operations required to initialize newly created experiment"""
        init_operations = list()

        # Assign experiment name
        init_operations.append(
            alpha_operation.AssignString(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_NAME_ATTRIBUTE_PATH),
                value=name,
            )
        )
        # Assign experiment description
        init_operations.append(
            alpha_operation.AssignString(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_DESCRIPTION_ATTRIBUTE_PATH),
                value=description,
            )
        )
        # Assign experiment parameters
        for p_name, p_val in params.items():
            parameter_type, string_value = self._get_parameter_with_type(p_val)
            operation_cls = alpha_operation.AssignFloat if parameter_type == "double" else alpha_operation.AssignString
            init_operations.append(
                operation_cls(
                    path=alpha_path_utils.parse_path(f"{alpha_consts.PARAMETERS_ATTRIBUTE_SPACE}{p_name}"),
                    value=string_value,
                )
            )
        # Assign experiment properties
        for p_key, p_val in properties.items():
            init_operations.append(
                AssignString(
                    path=alpha_path_utils.parse_path(f"{alpha_consts.PROPERTIES_ATTRIBUTE_SPACE}{p_key}"),
                    value=str(p_val),
                )
            )
        # Assign tags
        if tags:
            init_operations.append(
                alpha_operation.AddStrings(
                    path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
                    values=set(tags),
                )
            )
        # Assign source hostname
        if hostname:
            init_operations.append(
                alpha_operation.AssignString(
                    path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_HOSTNAME_ATTRIBUTE_PATH),
                    value=hostname,
                )
            )
        # Assign source entrypoint
        if entrypoint:
            init_operations.append(
                alpha_operation.AssignString(
                    path=alpha_path_utils.parse_path(alpha_consts.SOURCE_CODE_ENTRYPOINT_ATTRIBUTE_PATH),
                    value=entrypoint,
                )
            )

        return init_operations

    @legacy_with_api_exceptions_handler
    def reset_channel(self, experiment, channel_id, channel_name, channel_type):
        op = channel_type_to_clear_operation(ChannelType(channel_type))
        attr_path = self._get_channel_attribute_path(channel_name, ChannelNamespace.USER)
        self._execute_operations(
            experiment=experiment,
            operations=[op(path=alpha_path_utils.parse_path(attr_path))],
        )

    @legacy_with_api_exceptions_handler
    def _get_channel_tuples_from_csv(self, experiment, channel_attribute_path):
        try:
            csv = (
                self.leaderboard_swagger_client.api.getFloatSeriesValuesCSV(
                    experimentId=experiment.internal_id,
                    attribute=channel_attribute_path,
                )
                .response()
                .incoming_response.text
            )
            lines = csv.split("\n")[:-1]
            return [line.split(",") for line in lines]
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project.full_id,
            )

    @legacy_with_api_exceptions_handler
    def get_channel_points_csv(self, experiment, channel_internal_id, channel_name):
        try:
            channel_attr_path = self._get_channel_attribute_path(channel_name, ChannelNamespace.USER)
            values = self._get_channel_tuples_from_csv(experiment, channel_attr_path)
            step_and_value = [val[0] + "," + val[2] for val in values]
            csv = StringIO()
            for line in step_and_value:
                csv.write(line + "\n")
            csv.seek(0)
            return csv
        except HTTPNotFound:
            raise ExperimentNotFound(
                experiment_short_id=experiment.id,
                project_qualified_name=experiment._project.full_id,
            )

    @legacy_with_api_exceptions_handler
    def get_metrics_csv(self, experiment):
        metric_channels = [
            channel
            for channel in self._get_channels(experiment)
            if (
                channel.channelType == ChannelType.NUMERIC.value
                and channel.id.startswith(alpha_consts.MONITORING_ATTRIBUTE_SPACE)
            )
        ]
        data = {
            # val[1] + ',' + val[2] is timestamp,value
            ch.name: [val[1] + "," + val[2] for val in self._get_channel_tuples_from_csv(experiment, ch.id)]
            for ch in metric_channels
        }
        values_count = max(len(values) for values in data.values())
        csv = StringIO()
        csv.write(",".join(["x_{name},y_{name}".format(name=ch.name) for ch in metric_channels]))
        csv.write("\n")
        for i in range(0, values_count):
            csv.write(",".join([data[ch.name][i] if i < len(data[ch.name]) else "," for ch in metric_channels]))
            csv.write("\n")
        csv.seek(0)
        return csv

    @legacy_with_api_exceptions_handler
    def get_leaderboard_entries(
        self,
        project,
        entry_types=None,  # deprecated
        ids=None,
        states=None,
        owners=None,
        tags=None,
        min_running_time=None,
    ):
        if states is not None:
            states = [state if state == "running" else "idle" for state in states]
        try:

            def get_portion(limit, offset):
                return (
                    self.leaderboard_swagger_client.api.getLeaderboard(
                        projectIdentifier=project.full_id,
                        shortId=ids,
                        state=states,
                        owner=owners,
                        tags=tags,
                        tagsMode="and",
                        minRunningTimeSeconds=min_running_time,
                        sortBy=["sys/id"],
                        sortFieldType=["string"],
                        sortDirection=["ascending"],
                        limit=limit,
                        offset=offset,
                    )
                    .response()
                    .result.entries
                )

            return [
                LeaderboardEntry(self._to_leaderboard_entry_dto(e)) for e in self._get_all_items(get_portion, step=100)
            ]
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

    def websockets_factory(self, project_id, experiment_id):
        base_url = re.sub(r"^http", "ws", self.api_address) + "/api/notifications/v1"
        return ReconnectingWebsocketFactory(backend=self, url=base_url + f"/runs/{project_id}/{experiment_id}/signal")

    @staticmethod
    def _to_leaderboard_entry_dto(experiment_attributes):
        attributes = experiment_attributes.attributes
        system_attributes = experiment_attributes.systemAttributes

        def is_channel_namespace(name):
            return name.startswith(alpha_consts.LOG_ATTRIBUTE_SPACE) or name.startswith(
                alpha_consts.MONITORING_ATTRIBUTE_SPACE
            )

        numeric_channels = [
            HostedAlphaLeaderboardApiClient._float_series_to_channel_last_value_dto(attr)
            for attr in attributes
            if (
                attr.type == AttributeType.FLOAT_SERIES.value
                and is_channel_namespace(attr.name)
                and attr.floatSeriesProperties.last is not None
            )
        ]
        text_channels = [
            HostedAlphaLeaderboardApiClient._string_series_to_channel_last_value_dto(attr)
            for attr in attributes
            if (
                attr.type == AttributeType.STRING_SERIES.value
                and is_channel_namespace(attr.name)
                and attr.stringSeriesProperties.last is not None
            )
        ]

        return LegacyLeaderboardEntry(
            id=experiment_attributes.id,
            organizationName=experiment_attributes.organizationName,
            projectName=experiment_attributes.projectName,
            shortId=system_attributes.shortId.value,
            name=system_attributes.name.value,
            state="running" if system_attributes.state.value == "running" else "succeeded",
            timeOfCreation=system_attributes.creationTime.value,
            timeOfCompletion=None,
            runningTime=system_attributes.runningTime.value,
            owner=system_attributes.owner.value,
            size=system_attributes.size.value,
            tags=system_attributes.tags.values,
            description=system_attributes.description.value,
            channelsLastValues=numeric_channels + text_channels,
            parameters=[AlphaParameterDTO(attr) for attr in attributes if AlphaParameterDTO.is_valid_attribute(attr)],
            properties=[AlphaPropertyDTO(attr) for attr in attributes if AlphaPropertyDTO.is_valid_attribute(attr)],
        )

    @staticmethod
    def _float_series_to_channel_last_value_dto(attribute):
        return AlphaChannelWithValueDTO(
            channelId=attribute.name,
            channelName=attribute.name.split("/", 1)[-1],
            channelType="numeric",
            x=attribute.floatSeriesProperties.lastStep,
            y=attribute.floatSeriesProperties.last,
        )

    @staticmethod
    def _string_series_to_channel_last_value_dto(attribute):
        return AlphaChannelWithValueDTO(
            channelId=attribute.name,
            channelName=attribute.name.split("/", 1)[-1],
            channelType="text",
            x=attribute.stringSeriesProperties.lastStep,
            y=attribute.stringSeriesProperties.last,
        )

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []

        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items

        return items

    def _upload_raw_data(self, api_method, data, headers, path_params, query_params):
        url = self.api_address + api_method.operation.path_name + "?"

        for key, val in path_params.items():
            url = url.replace("{" + key + "}", val)

        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

        session = self.http_client.session

        request = self.authenticator.apply(requests.Request(method="POST", url=url, data=data, headers=headers))

        return handle_server_raw_response_messages(session.send(session.prepare_request(request)))

    def _get_parameter_with_type(self, parameter):
        string_type = "string"
        double_type = "double"
        if isinstance(parameter, bool):
            return (string_type, str(parameter))
        elif isinstance(parameter, float) or isinstance(parameter, int):
            if math.isinf(parameter) or math.isnan(parameter):
                return (string_type, json.dumps(parameter))
            else:
                return (double_type, str(parameter))
        else:
            return (string_type, str(parameter))

    def _convert_to_experiment(self, api_experiment, project):
        return Experiment(
            backend=project._backend,
            project=project,
            _id=api_experiment.shortId,
            internal_id=api_experiment.id,
        )

    def _download_raw_data(self, api_method, headers, path_params, query_params):
        url = self.api_address + api_method.operation.path_name + "?"

        for key, val in path_params.items():
            url = url.replace("{" + key + "}", val)

        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

        session = self.http_client.session

        request = self.authenticator.apply(requests.Request(method="GET", url=url, headers=headers))

        return handle_server_raw_response_messages(session.send(session.prepare_request(request), stream=True))
