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
from neptune import envs
from neptune.exceptions import NeptuneException, STYLES


class NeptuneApiException(NeptuneException):
    pass


class SSLError(NeptuneException):
    def __init__(self):
        super(SSLError, self).__init__('SSL certificate validation failed. Set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE '
                                       'environment variable to accept self-signed certificates.')


class ConnectionLost(NeptuneApiException):
    def __init__(self):
        super(ConnectionLost, self).__init__('Connection lost. Please try again.')


class ServerError(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----ServerError-----------------------------------------------------------------------
{end}
Neptune Client Library encountered an unexpected Server Error.

Please try again later or contact Neptune support.
"""
        super(ServerError, self).__init__(message.format(**STYLES))


class Unauthorized(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Unauthorized-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://docs-legacy.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
      
    - Verify if you set your Project qualified name correctly
      The correct project qualified name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
    - Ask your organization administrator to grant you necessary privileges to the project
"""
        super(Unauthorized, self).__init__(message.format(**STYLES))


class Forbidden(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Forbidden-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://docs-legacy.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
      
    - Verify if you set your Project qualified name correctly
      The correct project qualified name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
   - Ask your organization administrator to grant you necessary privileges to the project
"""
        super(Forbidden, self).__init__(message.format(**STYLES))


class InvalidApiKey(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----InvalidApiKey-----------------------------------------------------------------------
{end}
Your API token is invalid.
    
Learn how to get it in this docs page:
https://docs-legacy.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html

There are two options to add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass the token to {bold}neptune.init(){end} via {bold}api_token{end} argument:
    {python}neptune.init(project_qualified_name='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}=YOUR_API_TOKEN{end}
        
    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}=YOUR_API_TOKEN{end}
        
and skip the {bold}api_token{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(project_qualified_name='WORKSPACE_NAME/PROJECT_NAME'){end}
    
You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
    - https://docs-legacy.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
"""
        super(InvalidApiKey, self).__init__(message.format(
            env_api_token=envs.API_TOKEN_ENV_NAME,
            **STYLES
        ))


class WorkspaceNotFound(NeptuneApiException):
    def __init__(self, namespace_name):
        message = """
{h1}
----WorkspaceNotFound-------------------------------------------------------------------------
{end}
Workspace {python}{workspace}{end} not found.

Workspace is your username or a name of your team organization.
"""
        super(WorkspaceNotFound, self).__init__(message.format(
            workspace=namespace_name,
            **STYLES
        ))


class ProjectNotFound(NeptuneApiException):
    def __init__(self, project_identifier):
        message = """
{h1}
----ProjectNotFound-------------------------------------------------------------------------
{end}
Project {python}{project}{end} not found.

Verify if your project's name was not misspelled. You can find proper name after logging into Neptune UI.
"""
        super(ProjectNotFound, self).__init__(message.format(
            project=project_identifier,
            **STYLES
        ))


class PathInProjectNotFound(NeptuneApiException):
    def __init__(self, path, project_identifier):
        super(PathInProjectNotFound, self).__init__(
            "Path {} was not found in project {}.".format(path, project_identifier))


class PathInExperimentNotFound(NeptuneApiException):
    def __init__(self, path, exp_identifier):
        super().__init__(
            f"Path {path} was not found in experiment {exp_identifier}.")


class NotebookNotFound(NeptuneApiException):
    def __init__(self, notebook_id, project=None):
        if project:
            super(NotebookNotFound, self).__init__(
                "Notebook '{}' not found in project '{}'.".format(notebook_id, project))
        else:
            super(NotebookNotFound, self).__init__(
                "Notebook '{}' not found.".format(notebook_id))


class ExperimentNotFound(NeptuneApiException):
    def __init__(self, experiment_short_id, project_qualified_name):
        super(ExperimentNotFound, self).__init__("Experiment '{exp}' not found in '{project}'.".format(
            exp=experiment_short_id, project=project_qualified_name))


class ChannelNotFound(NeptuneApiException):
    def __init__(self, channel_id):
        super(ChannelNotFound, self).__init__("Channel '{id}' not found.".format(id=channel_id))


class ExperimentAlreadyFinished(NeptuneApiException):
    def __init__(self, experiment_short_id):
        super(ExperimentAlreadyFinished, self).__init__(
            "Experiment '{}' is already finished.".format(experiment_short_id))


class ExperimentLimitReached(NeptuneApiException):
    def __init__(self):
        super(ExperimentLimitReached, self).__init__('Experiment limit reached.')


class StorageLimitReached(NeptuneApiException):
    def __init__(self):
        super(StorageLimitReached, self).__init__('Storage limit reached.')


class ExperimentValidationError(NeptuneApiException):
    pass


class ChannelAlreadyExists(NeptuneApiException):
    def __init__(self, experiment_short_id, channel_name):
        super(ChannelAlreadyExists, self).__init__(
            "Channel with name '{}' already exists in experiment '{}'.".format(channel_name, experiment_short_id))


class ChannelDoesNotExist(NeptuneApiException):
    def __init__(self, experiment_short_id, channel_name):
        super(ChannelDoesNotExist, self).__init__(
            "Channel with name '{}' does not exist in experiment '{}'.".format(channel_name, experiment_short_id))


class ChannelsValuesSendBatchError(NeptuneApiException):
    @staticmethod
    def _format_error(error):
        return "{msg} (metricId: '{channelId}', x: {x})".format(
            msg=error.error,
            channelId=error.channelId,
            x=error.x)

    def __init__(self, experiment_short_id, batch_errors):
        super(ChannelsValuesSendBatchError, self).__init__(
            "Received batch errors sending channels' values to experiment {}. "
            "Cause: {} "
            "Skipping {} values.".format(
                experiment_short_id,
                self._format_error(batch_errors[0]) if batch_errors else "No errors",
                len(batch_errors)))


class ExperimentOperationErrors(NeptuneApiException):
    """Handles minor errors returned by calling `client.executeOperations`"""
    def __init__(self, errors):
        super().__init__()
        self.errors = errors

    def __str__(self):
        lines = ['Caused by:']
        for error in self.errors:
            lines.append(f'\t* {error}')
        return '\n'.join(lines)
