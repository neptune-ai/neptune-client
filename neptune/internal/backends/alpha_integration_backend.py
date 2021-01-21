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
import dateutil
import six
from bravado.exception import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity
from mock import NonCallableMagicMock

from neptune.alpha.internal import operation as alpha_operation
from neptune.alpha.internal.backends.api_model import AttributeType as AlphaAttributeType
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend as AlphaHostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials as AlphaCredentials
from neptune.alpha.internal.utils import paths as alpha_path_utils
from neptune.api_exceptions import (
    ExperimentLimitReached,
    ExperimentValidationError,
    ProjectNotFound, ExperimentNotFound,
)
from neptune.exceptions import STYLES
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.utils.alpha_integration import MONITORING_ATTRIBUTE_SPACE, PARAMETERS_ATTRIBUTE_SPACE
from neptune.internal.utils.http import extract_response_field
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

    def _create_experiment_operations(self, entrypoint, params, tags) -> List[alpha_operation.Operation]:
        """Returns operations required to initialize newly created experiment"""
        init_operations = list()

        # Assign source entrypoint
        init_operations.append(alpha_operation.AssignString(
            path=['source_code', 'entrypoint'],
            value=entrypoint,
        ))
        # Assign experiment parameters
        for p_name, p_val in params.items():
            parameter_type, string_value = self._get_parameter_with_type(p_val)
            operation_cls = alpha_operation.AssignFloat if parameter_type == 'double' else alpha_operation.AssignString
            init_operations.append(operation_cls(
                path=alpha_path_utils.parse_path(f'{PARAMETERS_ATTRIBUTE_SPACE}{p_name}'),
                value=string_value,
            ))
        # Assign tags
        init_operations.append(alpha_operation.AddStrings(
            path=['sys', 'tags'],
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
            "customId": name,
        }

        kwargs = {
            'experimentCreationParams': api_params,
            'X-Neptune-CliVersion': self.client_lib_version,
        }
        api_experiment = self.leaderboard_swagger_client.api.createExperiment(**kwargs).response().result

        try:
            # TODO: handle alpha exceptions
            self._alpha_backend.execute_operations(
                experiment_uuid=uuid.UUID(api_experiment.id),
                operations=self._create_experiment_operations(entrypoint, params, tags),
            )
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
    def send_channels_values(self, experiment, channels_with_values):
        ops = []
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
                    step=None,  # TODO: what is step?
                    ts=int(value.ts),
                )
                for value in channel_with_values.channel_values
            ]
            ops.append(alpha_operation.LogStrings(
                path=alpha_path_utils.parse_path(channel_with_values.channel_id),
                values=ch_values,
            ))

        try:
            # TODO: handle alpha exceptions
            errors = self._alpha_backend.execute_operations(
                experiment_uuid=uuid.UUID(experiment.internal_id),
                operations=ops
            )
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

    def get_system_channels(self, experiment):
        params = {
            'experimentId': experiment.internal_id,
        }
        try:
            experiment = self.leaderboard_swagger_client.api.getExperimentAttributes(**params).response().result
            return [
                AlphaChannelWithLastValue(
                    ch_id=attr.stringSeriesProperties.attributeName,
                    ch_name=alpha_path_utils.parse_path(attr.stringSeriesProperties.attributeName)[-1],
                    ch_type=attr.stringSeriesProperties.attributeType,
                )
                for attr in experiment.attributes
                if (attr.type == AlphaAttributeType.STRING_SERIES.value
                    and attr.name.startswith(MONITORING_ATTRIBUTE_SPACE))
            ]
        except HTTPNotFound:
            # pylint: disable=protected-access
            raise ExperimentNotFound(
                experiment_short_id=experiment.id, project_qualified_name=experiment._project.full_id)

    def create_system_channel(self, experiment, name, channel_type):
        dummy_log_string = alpha_operation.LogStrings(
            path=alpha_path_utils.parse_path(f'{MONITORING_ATTRIBUTE_SPACE}{name}'),
            values=[],
        )
        # pylint: disable=unused-variable
        errors = self._alpha_backend.execute_operations(
            experiment_uuid=uuid.UUID(experiment.internal_id),
            operations=[dummy_log_string],
        )
        system_channels = self.get_system_channels(experiment)
        for channel in system_channels:
            if channel.name == name:
                return channel
        raise Exception()

    def upload_experiment_source(self, experiment, data, progress_indicator):
        # TODO: handle `FileChunkStream` or update `neptune.experiments.Experiment._start`
        pass

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        experiment = super().get_experiment(experiment_id)
        fake_experiment = NonCallableMagicMock()
        # `timeOfCreation` is required by `TimeOffsetGenerator`
        fake_experiment.timeOfCreation = dateutil.parser.parse(experiment.creationTime)
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
