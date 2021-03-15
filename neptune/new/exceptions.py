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

from neptune.new import envs
from neptune.new.envs import CUSTOM_EXP_ID_ENV_NAME
from neptune.new.internal.utils import replace_patch_version
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
        message = """
{h1}
----InternalClientError-----------------------------------------------------------------------
{end}
Neptune Client Library encountered an unexpected Internal Error:
{msg}

Please contact Neptune support.

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({"msg": msg}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


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
    - https://docs-beta.neptune.ai/administration/workspace-project-and-user-management
    - https://docs-beta.neptune.ai/getting-started/quick-starts/hello-world#step-2-create-a-quickstart-py

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({'env_project': envs.PROJECT_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class NeptuneIncorrectProjectNameException(NeptuneException):
    def __init__(self, project):
        message = """
{h1}
----NeptuneIncorrectProjectNameException-----------------------------------------------------------------------
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
    - https://docs-beta.neptune.ai/administration/workspace-project-and-user-management
    - https://docs-beta.neptune.ai/getting-started/quick-starts/hello-world#step-2-create-a-quickstart-py

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
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
https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token

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
    - https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token
    - https://docs-beta.neptune.ai/getting-started/quick-starts/hello-world#step-2-create-a-quickstart-py

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
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
https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token

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
    - https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token
    - https://docs-beta.neptune.ai/getting-started/quick-starts/hello-world#step-2-create-a-quickstart-py

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({'env_api_token': envs.API_TOKEN_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class CannotSynchronizeOfflineExperimentsWithoutProject(NeptuneException):
    def __init__(self):
        super().__init__("Cannot synchronize offline experiments without a project.")


class NeptuneExperimentResumeAndCustomIdCollision(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneExperimentResumeAndCustomIdCollision-----------------------------------------------------------------------
{end}
Incorrect call of function {python}neptune.init(){end}.

Parameters {python}experiment{end} and {python}custom_experiment_id{end} of {python}neptune.init(){end} are mutually exclusive.
Make sure you have no {bash}{custom_id_env}{end} environment variable defined
and no value explicitly passed to `custom_experiment_id` argument if you meant to resume experiment.

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({'custom_id_env': CUSTOM_EXP_ID_ENV_NAME}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class UnsupportedClientVersion(NeptuneException):
    def __init__(
            self,
            version: Union[Version, str],
            min_version: Optional[Union[Version, str]] = None,
            max_version: Optional[Union[Version, str]] = None):
        current_version = str(version)
        required_version = "==" + replace_patch_version(str(max_version)) if max_version else ">=" + str(min_version)
        message = """
{h1}
----UnsupportedClientVersion-----------------------------------------------------------------------
{end}
Your version of neptune-client ({current_version}) library is not supported by this Neptune server.

Please install neptune-client{required_version}

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list(
            {'current_version': current_version, 'required_version': required_version}.items()
        ) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        message = """
{h1}
----CannotResolveHostname-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to resolve hostname {underline}{host}{end}.

What should I do?
    - Check if your computer is connected to the internet.
    - Check if your computer should use any proxy to access internet.
      If so, you may want to use {python}proxies{end} parameter of {python}neptune.init(){end} function.
      See (TODO: paste docs link here)
      and https://requests.readthedocs.io/en/master/user/advanced/#proxies

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({'host': host}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


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
        message = """
{h1}
----InternalServerError-----------------------------------------------------------------------
{end}
Neptune Client Library encountered an unexpected Internal Server Error.

Please try again later or contact Neptune support.

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class Unauthorized(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Unauthorized-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token
      
    - Verify if you set up your project correctly
      The correct project name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
   - Ask your organization administrator to grant you necessary privileges to the project

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class Forbidden(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Forbidden-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://docs-beta.neptune.ai/administration/security-and-privacy/how-to-find-and-set-neptune-api-token
      
    - Verify if you set up your project correctly
      The correct project name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
   - Ask your organization administrator to grant you necessary privileges to the project

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class OfflineModeFetchException(NeptuneException):
    def __init__(self):
        super().__init__('It is not possible to fetch data from the server in offline mode')


class OperationNotSupported(NeptuneException):
    def __init__(self, message: str):
        super().__init__('Operation not supported: {}'.format(message))


class OldProjectException(NeptuneException):
    def __init__(self, project: str):
        super().__init__('{} is an old Neptune Project. Do not use `neptune.new` module to work with this project.'
                         'Use old client from `neptune` module instead.'.format(project))


class NeptuneUninitializedException(NeptuneException):
    def __init__(self):
        message = """
{h1}     
----NeptuneUninitializedException---------------------------------------------------------------------------------------
{end}
You must initialize neptune-client before you access `get_last_exp`.

Looks like you forgot to add:
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

before you ran:
    {python}neptune.get_last_exp(){end}

You may also want to check the following docs pages:
    - TODO: paste docs link here

{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
""".format(**STYLES)
        super().__init__(message)


class NeptuneIntegrationNotInstalledException(NeptuneException):
    def __init__(self, framework):
        message = """
{h1}     
----NeptuneIntegrationNotInstalledException-----------------------------------------
{end}
Looks like integration neptune-{framework} wasn't installed.
To install run:
    {bash}pip install neptune-{framework}{end}
Or:
    {bash}pip install neptune-client[{framework}]{end}

You may also want to check the following docs pages:
    - https://docs-beta.neptune.ai/essentials/integrations
    
{correct}Need help?{end}-> https://docs-beta.neptune.ai/getting-started/getting-help
"""
        inputs = dict(list({'framework': framework}.items()) + list(STYLES.items()))
        super().__init__(message.format(**inputs))


class PlotlyIncompatibilityException(Exception):
    def __init__(self, matplotlib_version, plotly_version):
        super().__init__(
            "Unable to convert plotly figure to matplotlib format. "
            "Your matplotlib ({}) and plotlib ({}) versions are not compatible. "
            "See https://stackoverflow.com/q/63120058 for details. "
            "Downgrade matplotlib to version 3.2 or use as_image to log static chart."
            .format(matplotlib_version, plotly_version))
