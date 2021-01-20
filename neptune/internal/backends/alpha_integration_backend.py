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

import click
import dateutil
import six
from bravado.exception import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity
from mock import NonCallableMagicMock

from neptune.alpha.internal import operation as alpha_operation
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
from neptune.internal.utils.http import extract_response_field
from neptune.model import AlphaChannelWithLastValue
from neptune.projects import Project
from neptune.utils import with_api_exceptions_handler

_logger = logging.getLogger(__name__)


class AlphaIntegrationBackend(HostedNeptuneBackend):
    def __init__(self, api_token=None, proxies=None):
        super().__init__(api_token, proxies)
        self._alpha_backend = AlphaHostedNeptuneBackend(AlphaCredentials(api_token=api_token))
        self._system_channels = list()

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

        params = {
            "projectIdentifier": str(project.internal_id),
            "cliVersion": self.client_lib_version,
            "gitInfo": git_info,
            "customId": name,
        }

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': self.client_lib_version,
        }
        api_experiment = self.leaderboard_swagger_client.api.createExperiment(**kwargs).response().result

        upload_src_op = alpha_operation.AssignString(
            path=['source_code', 'entrypoint'],
            value=entrypoint,
        )
        add_tags_op = alpha_operation.AddStrings(
            path=['sys', 'tags'],
            values=set(tags),
        )

        try:
            # TODO: handle alpha exceptions
            self._alpha_backend.execute_operations(
                experiment_uuid=uuid.UUID(api_experiment.id),
                operations=[upload_src_op, add_tags_op]
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
        return self._system_channels

    def create_system_channel(self, experiment, name, channel_type):
        new_channel = AlphaChannelWithLastValue(
            ch_id=f'monitoring/{name}',
            ch_name=name,
            ch_type=channel_type,
        )
        self._system_channels.append(new_channel)
        return new_channel

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
