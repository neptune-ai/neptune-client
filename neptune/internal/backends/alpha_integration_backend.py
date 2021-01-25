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
import uuid
from typing import List

import click
import six
from bravado.exception import HTTPNotFound
from mock import NonCallableMagicMock

from neptune.alpha import exceptions as alpha_exceptions
from neptune.alpha.attributes import constants as alpha_consts
from neptune.alpha.internal import operation as alpha_operation
from neptune.alpha.internal.backends.api_model import AttributeType as AlphaAttributeType
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend as AlphaHostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials as AlphaCredentials
from neptune.alpha.internal.utils import paths as alpha_path_utils
from neptune.api_exceptions import (
    AlphaOperationErrors,
    ChannelNotFound,
    ExperimentNotFound,
    ProjectNotFound,
)
from neptune.exceptions import STYLES, NeptuneException
from neptune.experiments import Experiment
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.model import AlphaChannelWithLastValue
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
    def send_channels_values(self, experiment, channels_with_values):
        send_operations = []
        for channel_with_values in channels_with_values:
            # TODO: handle other data types
            # points = [Point(
            #     timestampMillis=int(value.ts * 1000.0),
            #     x=value.x,
            #     y=Y(numericValue=value.y.get('numeric_value'),
            #         textValue=value.y.get('text_value'),
            #         inputImageValue=value.y.get('image_value'))
            # ) for value in channel_with_values.channel_values]
            ch_values = [
                alpha_operation.LogStrings.ValueType(
                    value=value.y.get('text_value'),
                    step=None,
                    ts=value.ts,
                )
                for value in channel_with_values.channel_values
            ]
            send_operations.append(alpha_operation.LogStrings(
                path=alpha_path_utils.parse_path(channel_with_values.channel_id),
                values=ch_values,
            ))

        self._execute_alpha_operation(experiment, send_operations)

    @with_api_exceptions_handler
    def get_system_channels(self, experiment):
        params = {
            'experimentId': experiment.internal_id,
        }
        try:
            experiment = self.leaderboard_swagger_client.api.getExperimentAttributes(**params).response().result
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)
        return [
            AlphaChannelWithLastValue(
                ch_id=attr.stringSeriesProperties.attributeName,
                # ch_name is ch_id without first namespace
                ch_name=attr.stringSeriesProperties.attributeName.split('/', 1)[-1],
                ch_type=attr.stringSeriesProperties.attributeType,
            )
            for attr in experiment.attributes
            if (attr.type == AlphaAttributeType.STRING_SERIES.value
                and attr.name.startswith(alpha_consts.MONITORING_ATTRIBUTE_SPACE))
        ]

    @with_api_exceptions_handler
    def create_system_channel(self, experiment, name, channel_type):
        channel_id = f'{alpha_consts.MONITORING_ATTRIBUTE_SPACE}{name}'
        dummy_log_string = alpha_operation.LogStrings(
            path=alpha_path_utils.parse_path(channel_id),
            values=[],
        )  # this operation is used to create empty attribute
        self._execute_alpha_operation(
            experiment=experiment,
            operations=[dummy_log_string],
        )
        system_channels = self.get_system_channels(experiment)
        for channel in system_channels:
            if channel.name == name:
                return channel
        raise ChannelNotFound(channel_id=channel_id)

    def upload_experiment_source(self, experiment, data, progress_indicator):
        # TODO: handle `FileChunkStream` or update `neptune.experiments.Experiment._start`
        pass

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        experiment = self.leaderboard_swagger_client.api.getExperiment(experimentId=experiment_id).response().result
        fake_experiment = NonCallableMagicMock()
        # `timeOfCreation` is required by `TimeOffsetGenerator`
        fake_experiment.timeOfCreation = experiment.creationTime
        return fake_experiment

    def create_hardware_metric(self, experiment, metric):
        pass

    def mark_succeeded(self, experiment):
        pass

    def ping_experiment(self, experiment):
        pass

    @staticmethod
    def _get_client_config_args(api_token):
        return dict(
            X_Neptune_Api_Token=api_token,
            alpha="true",
        )
