#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import uuid
from typing import Union, Optional, List

from packaging.version import Version

from neptune.alpha import envs
from neptune.alpha.envs import CUSTOM_EXP_ID_ENV_NAME
from neptune.alpha.internal.utils import replace_patch_version
from neptune.exceptions import STYLES


class NeptuneException(Exception):

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and str(self).__eq__(str(other))
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), str(self)))


class NeptuneApiException(NeptuneException):
    pass


class MetadataInconsistency(NeptuneException):
    pass


class MalformedOperation(NeptuneException):
    pass


class FileNotFound(NeptuneException):
    def __init__(self, file: str):
        super().__init__("File not found: {}".format(file))


class FileUploadError(NeptuneException):
    def __init__(self, filename: str, msg: str):
        super().__init__("Cannot upload file {}: {}".format(filename, msg))


class FileSetUploadError(NeptuneException):
    def __init__(self, globs: List[str], msg: str):
        super().__init__("Cannot upload file set {}: {}".format(globs, msg))


class InternalClientError(NeptuneException):
    def __init__(self, msg: str):
        super().__init__("Internal client error: {}. Please contact Neptune support.".format(msg))


class ClientHttpError(NeptuneException):
    def __init__(self, code: int):
        super().__init__("Client HTTP error {}".format(code))


class ProjectNotFound(NeptuneException):
    def __init__(self, project_id):
        super().__init__("Project {} not found.".format(project_id))


class ExperimentNotFound(NeptuneException):

    def __init__(self, experiment_id: str) -> None:
        super().__init__("Experiment {} not found.".format(experiment_id))


class ExperimentUUIDNotFound(NeptuneException):
    def __init__(self, exp_uuid: uuid.UUID):
        super().__init__("Experiment with UUID {} not found. Could be deleted.".format(exp_uuid))


class NeptuneMissingProjectNameException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingProjectNameException-------------------------------------------------------------------------
{end}
Neptune client couldn't find your project name.

There are two options two add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass it to {bold}neptune.init(){end} via {bold}project{end} argument:
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME'){end}

{h2}ENVIRONMENT VARIABLE{end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
       {bash}export {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

    {correct}Windows{end}
    In your CMD run:
       {bash}set {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

and skip the {bold}project{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'env_project': envs.PROJECT_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class NeptuneIncorrectProjectQualifiedNameException(NeptuneException):
    def __init__(self, project):
        message = """
{h1}
----NeptuneIncorrectProjectQualifiedNameException-----------------------------------------------------------------------
{end}
Project qualified name {fail}"{project}"{end} you specified was incorrect.

The correct project qualified name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
It has two parts:
    - {correct}WORKSPACE{end}: which can be your username or your organization name
    - {correct}PROJECT_NAME{end}: which is the actual project name you chose 

For example, a project {correct}neptune-ai/credit-default-prediction{end} parts are:
    - {correct}neptune-ai{end}: {underline}WORKSPACE{end} our company organization name
    - {correct}credit-default-prediction{end}: {underline}PROJECT_NAME{end} a project name

The URL to this project looks like this: https://ui.neptune.ai/neptune-ai/credit-default-prediction

You may also want to check the following docs pages:
    - https://docs.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'project': project}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class NeptuneMissingApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingApiTokenException-------------------------------------------------------------------------------------
{end}
Neptune client couldn't find your API token.

Learn how to get it in this docs page:
https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html

There are two options to add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass the token to {bold}neptune.init(){end} via {bold}api_token{end} argument:
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}="YOUR_API_TOKEN"{end}

and skip the {bold}api_token{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME'){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'env_api_token': envs.API_TOKEN_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class NeptuneInvalidApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneInvalidApiTokenException-------------------------------------------------------------------------------------
{end}
Provided API token is invalid.
Make sure you copied and provided your API token correctly.

Learn how to get it in this docs page:
https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html

There are two options to add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass the token to {bold}neptune.init(){end} via {bold}api_token{end} argument:
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}="YOUR_API_TOKEN"{end}

and skip the {bold}api_token{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME'){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'env_api_token': envs.API_TOKEN_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class NeptuneExperimentResumeAndCustomIdCollision(NeptuneException):
    def __init__(self):
        super().__init__("`experiment` and `custom_experiment_id` arguments of init() function are mutually exclusive. "
                         "Make sure you have no {custom_id_env} environment variable defined "
                         "and no value explicitly passed to `custom_experiment_id` argument "
                         "if you meant to resume experiment."
                         .format(custom_id_env=CUSTOM_EXP_ID_ENV_NAME))


class UnsupportedClientVersion(NeptuneException):
    def __init__(
            self,
            version: Union[Version, str],
            min_version: Optional[Union[Version, str]] = None,
            max_version: Optional[Union[Version, str]] = None):
        super().__init__(
            "This neptune-client version ({}) is not supported. Please install neptune-client{}".format(
                str(version),
                "==" + replace_patch_version(str(max_version)) if max_version else ">=" + str(min_version)
            ))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        super().__init__("Cannot resolve hostname {}.".format(host))


class SSLError(NeptuneException):
    def __init__(self):
        super().__init__(
            'SSL certificate validation failed. Set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE '
            'environment variable to accept self-signed certificates.')


class ConnectionLost(NeptuneException):
    def __init__(self):
        super().__init__('Connection to Neptune server was lost.')


class InternalServerError(NeptuneApiException):
    def __init__(self):
        super().__init__('Internal server error. Please contact Neptune support.')


class Unauthorized(NeptuneApiException):
    def __init__(self):
        super().__init__('Unauthorized. Verify your API token is invalid.')


class Forbidden(NeptuneApiException):
    def __init__(self):
        super().__init__('You have no permissions to access this resource.')


class OfflineModeFetchException(NeptuneException):
    def __init__(self):
        super().__init__('It is not possible to fetch data from the server in offline mode')


class OperationNotSupported(NeptuneException):
    def __init__(self, message: str):
        super().__init__('Operation not supported: {}'.format(message))


class NotAlphaProjectException(NeptuneException):
    def __init__(self, project: str):
        super().__init__('{} is not Alpha Neptune Project. Use old neptune-client from neptune package'.format(project))
