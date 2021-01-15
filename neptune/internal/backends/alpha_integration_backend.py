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

import click
import six
from bravado.exception import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity
from mock import NonCallableMagicMock

from neptune.api_exceptions import ExperimentLimitReached, \
    ExperimentValidationError, ProjectNotFound
from neptune.exceptions import STYLES
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.utils.http import extract_response_field
from neptune.projects import Project
from neptune.utils import with_api_exceptions_handler

_logger = logging.getLogger(__name__)


class AlphaIntegrationBackend(HostedNeptuneBackend):
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

        try:
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

    def get_system_channels(self, experiment):
        return list()

    def create_system_channel(self, experiment, name, channel_type):
        return NonCallableMagicMock()

    def upload_experiment_source(self, experiment, data, progress_indicator):
        pass

    @with_api_exceptions_handler
    def get_experiment(self, experiment_id):
        return NonCallableMagicMock()

    def create_hardware_metric(self, experiment, metric):
        pass

    def mark_succeeded(self, experiment):
        pass

    @staticmethod
    def _get_client_config_args(api_token):
        return dict(
            X_Neptune_Api_Token=api_token,
            alpha="true",
        )
