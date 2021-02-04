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
import logging
import os
import uuid
from typing import List, Dict

import click
import six
from bravado.exception import HTTPNotFound
from mock import NonCallableMagicMock

from neptune.alpha import exceptions as alpha_exceptions
from neptune.alpha.attributes import constants as alpha_consts
from neptune.alpha.internal import operation as alpha_operation
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend as AlphaHostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials as AlphaCredentials
from neptune.alpha.internal.utils import paths as alpha_path_utils, base64_encode
from neptune.api_exceptions import (
    AlphaOperationErrors,
    ExperimentNotFound,
    ProjectNotFound,
)
from neptune.exceptions import STYLES, NeptuneException, FileNotFound
from neptune.experiments import Experiment
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.channels.channels import ChannelType, ChannelValueType
from neptune.internal.storage.storage_utils import normalize_file_name
from neptune.internal.utils.alpha_integration import (
    AlphaChannelDTO,
    AlphaChannelWithValueDTO,
    AlphaPropertyDTO,
    channel_type_to_operation,
    channel_value_type_to_operation,
    deprecated_img_to_alpha_image,
)
from neptune.model import ChannelWithLastValue
from neptune.projects import Project
from neptune.utils import with_api_exceptions_handler

_logger = logging.getLogger(__name__)


class AlphaIntegrationBackend(HostedNeptuneBackend):
    def __init__(self, api_token=None, proxies=None):
        super().__init__(api_token, proxies)
        self._alpha_backend = AlphaHostedNeptuneBackend(AlphaCredentials(api_token=api_token))

    @with_api_exceptions_handler
    def get_project(self, project_qualified_name):
        try:
            response = self.backend_swagger_client.api.getProject(projectIdentifier=project_qualified_name).response()
            warning = response.metadata.headers.get('X-Server-Warning')
            if warning:
                click.echo('{warning}{content}{end}'.format(content=warning, **STYLES))
            project = response.result

            return Project(
                backend=self,
                internal_id=project.id,
                namespace=project.organizationName,
                name=project.name)
        except HTTPNotFound:
            raise ProjectNotFound(project_qualified_name)

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

        git_info = {
            "commit": {
                "commitId": git_info.commit_id,
                "message": git_info.message,
                "authorName": git_info.author_name,
                "authorEmail": git_info.author_email,
                "commitDate": git_info.commit_date
            },
            "repositoryDirty": git_info.repository_dirty,
            "currentBranch": git_info.active_branch,
            "remotes": git_info.remote_urls
        } if git_info else None

        api_params = {
            "projectIdentifier": str(project.internal_id),
            "cliVersion": self.client_lib_version,
            "gitInfo": git_info,
            "customId": None,
        }

        kwargs = {
            'experimentCreationParams': api_params,
            'X-Neptune-CliVersion': self.client_lib_version,
        }

        try:
            api_experiment = self.leaderboard_swagger_client.api.createExperiment(**kwargs).response().result
        except HTTPNotFound:
            raise ProjectNotFound(project_identifier=project.full_id)

        experiment = self._convert_to_experiment(api_experiment, project)
        # Initialize new experiment
        self._execute_alpha_operation(
            experiment=experiment,
            operations=self._get_init_experiment_operations(name, entrypoint, params, tags),
        )
        return experiment

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        experiment = self.leaderboard_swagger_client.api.getExperiment(experimentId=experiment_id).response().result
        fake_experiment = NonCallableMagicMock()
        # `timeOfCreation` is required by `TimeOffsetGenerator`
        fake_experiment.timeOfCreation = experiment.creationTime

        try:
            tags = self._alpha_backend.get_string_set_attribute(
                experiment_uuid=uuid.UUID(experiment_id),
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
            ).values
        except alpha_exceptions.MetadataInconsistency:
            tags = list()
        fake_experiment.tags = tags
        fake_experiment.properties = [
            AlphaPropertyDTO(attr) for attr in self._get_attributes(experiment_id)
            if AlphaPropertyDTO.is_valid_attribute(attr)
        ]

        return fake_experiment

    def update_experiment(self, experiment, properties):
        raise NeptuneException("`update_experiment` shouldn't be called.")

    @with_api_exceptions_handler
    def set_property(self, experiment, key, value):
        """Save attribute casted to string under `alpha_consts.PROPERTIES_ATTRIBUTE_SPACE` namespace"""
        self._execute_alpha_operation(
            experiment=experiment,
            operations=[alpha_operation.AssignString(
                path=alpha_path_utils.parse_path(f'{alpha_consts.PROPERTIES_ATTRIBUTE_SPACE}{key}'),
                value=str(value),
            )],
        )

    @with_api_exceptions_handler
    def remove_property(self, experiment, key):
        self._remove_attribute(experiment, str_path=f'{alpha_consts.PROPERTIES_ATTRIBUTE_SPACE}{key}')

    @with_api_exceptions_handler
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        operations = [
            alpha_operation.AddStrings(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
                values=tags_to_add,
            ),
            alpha_operation.RemoveStrings(
                path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
                values=tags_to_delete,
            )
        ]
        self._execute_alpha_operation(
            experiment=experiment,
            operations=operations,
        )

    def upload_experiment_source(self, experiment, data, progress_indicator):
        # TODO: handle `FileChunkStream` or update `neptune.experiments.Experiment._start`
        pass

    def _create_channel(self, experiment: Experiment, channel_id: str, channel_name: str, channel_type: str):
        """This function is responsible for creating 'fake' channels in alpha projects.

        Since channels are abandoned in alpha api, we're mocking them using empty logging operation."""

        operation = channel_type_to_operation(ChannelType(channel_type))

        log_empty_operation = operation(
            path=alpha_path_utils.parse_path(channel_id),
            values=[],
        )  # this operation is used to create empty attribute
        self._execute_alpha_operation(
            experiment=experiment,
            operations=[log_empty_operation],
        )
        return ChannelWithLastValue(
            AlphaChannelWithValueDTO(
                channelId=channel_id,
                channelName=channel_name,
                channelType=channel_type,
                x=None,
                y=None
            )
        )

    @with_api_exceptions_handler
    def create_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        channel_id = f'{alpha_consts.LOG_ATTRIBUTE_SPACE}{name}'
        return self._create_channel(experiment, channel_id,
                                    channel_name=name,
                                    channel_type=channel_type)

    def _get_channels(self, experiment) -> List[AlphaChannelDTO]:
        return [
            AlphaChannelDTO(attr) for attr in self._get_attributes(experiment.internal_id)
            if AlphaChannelDTO.is_valid_attribute(attr)
        ]

    @with_api_exceptions_handler
    def get_channels(self, experiment) -> Dict[str, AlphaChannelDTO]:
        api_channels = [
            channel for channel in self._get_channels(experiment)
            # return channels from LOG_ATTRIBUTE_SPACE namespace only
            if channel.id.startswith(alpha_consts.LOG_ATTRIBUTE_SPACE)
        ]
        channels = dict()
        for ch in api_channels:
            # TODO: NPT-9216
            ch.x = ch.lastX
            ch.y = ch.lastY
            channels[ch.name] = ch
        return channels

    @with_api_exceptions_handler
    def create_system_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        channel_id = f'{alpha_consts.MONITORING_ATTRIBUTE_SPACE}{name}'
        return self._create_channel(experiment, channel_id,
                                    channel_name=name,
                                    channel_type=ChannelType.TEXT.value)

    @with_api_exceptions_handler
    def get_system_channels(self, experiment) -> Dict[str, AlphaChannelDTO]:
        return {
            channel.name: channel
            for channel in self._get_channels(experiment)
            if (channel.channelType == ChannelType.TEXT.value
                and channel.id.startswith(alpha_consts.MONITORING_ATTRIBUTE_SPACE))
        }

    @with_api_exceptions_handler
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
                data_transformer = lambda e: e

            ch_values = [
                alpha_operation.LogSeriesValue(
                    value=data_transformer(ch_value.value),
                    step=None,
                    ts=ch_value.ts,
                )
                for ch_value in channel_with_values.channel_values
            ]
            send_operations.append(operation(
                path=alpha_path_utils.parse_path(channel_with_values.channel_id),
                values=ch_values,
            ))

        self._execute_alpha_operation(experiment, send_operations)

    def mark_succeeded(self, experiment):
        pass

    def ping_experiment(self, experiment):
        pass

    def create_hardware_metric(self, experiment, metric):
        pass

    def log_artifact(self, experiment, artifact, destination=None):
        target_name = os.path.basename(artifact) if destination is None else destination
        target_name = f'{alpha_consts.ARTIFACT_ATTRIBUTE_SPACE}{target_name}'
        dest_path = alpha_path_utils.parse_path(normalize_file_name(target_name))
        if isinstance(artifact, str):
            if os.path.exists(artifact):
                operations = [alpha_operation.UploadFile(
                    path=dest_path,
                    file_name=dest_path[-1],
                    file_path=os.path.abspath(artifact),
                )]
            else:
                raise FileNotFound(artifact)
        elif hasattr(artifact, 'read'):
            if destination is not None:
                operations = [alpha_operation.UploadFileContent(
                    path=dest_path,
                    file_name=dest_path[-1],
                    file_content=base64_encode(artifact.read().encode('utf-8')),
                )]
            else:
                raise ValueError("destination is required for file streams")
        else:
            raise ValueError("Artifact must be a local path or an IO object")

        self._execute_alpha_operation(experiment, operations)

    def delete_artifacts(self, experiment, path):
        self._remove_attribute(experiment, str_path=f'{alpha_consts.ARTIFACT_ATTRIBUTE_SPACE}{path}')

    def _get_attributes(self, experiment_id) -> list:
        params = {
            'experimentId': experiment_id,
        }
        try:
            experiment = self.leaderboard_swagger_client.api.getExperimentAttributes(**params).response().result
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

        return experiment.attributes

    def _remove_attribute(self, experiment, str_path: str):
        """Removes given attribute"""
        self._execute_alpha_operation(
            experiment=experiment,
            operations=[alpha_operation.DeleteAttribute(
                path=alpha_path_utils.parse_path(str_path),
            )],
        )

    @staticmethod
    def _get_client_config_args(api_token):
        return dict(
            X_Neptune_Api_Token=api_token,
            alpha="true",
        )

    def _execute_alpha_operation(self, experiment: Experiment, operations: List[alpha_operation.Operation]):
        """Execute operations using alpha backend"""
        try:
            errors = self._alpha_backend.execute_operations(
                experiment_uuid=uuid.UUID(experiment.internal_id),
                operations=operations
            )
            if errors:
                raise AlphaOperationErrors(errors)
        except alpha_exceptions.ExperimentUUIDNotFound as e:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id) from e
        except alpha_exceptions.InternalClientError as e:
            raise NeptuneException(e) from e

    def _get_init_experiment_operations(self, name, entrypoint, params, tags) -> List[alpha_operation.Operation]:
        """Returns operations required to initialize newly created experiment"""
        init_operations = list()

        # Assign experiment name
        init_operations.append(alpha_operation.AssignString(
            path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_NAME_ATTRIBUTE_PATH),
            value=name,
        ))
        # Assign source entrypoint
        init_operations.append(alpha_operation.AssignString(
            path=alpha_path_utils.parse_path(alpha_consts.SOURCE_CODE_ENTRYPOINT_ATTRIBUTE_PATH),
            value=entrypoint,
        ))
        # Assign experiment parameters
        for p_name, p_val in params.items():
            parameter_type, string_value = self._get_parameter_with_type(p_val)
            operation_cls = alpha_operation.AssignFloat if parameter_type == 'double' else alpha_operation.AssignString
            init_operations.append(operation_cls(
                path=alpha_path_utils.parse_path(f'{alpha_consts.PARAMETERS_ATTRIBUTE_SPACE}{p_name}'),
                value=string_value,
            ))
        # Assign tags
        init_operations.append(alpha_operation.AddStrings(
            path=alpha_path_utils.parse_path(alpha_consts.SYSTEM_TAGS_ATTRIBUTE_PATH),
            values=set(tags),
        ))

        return init_operations
