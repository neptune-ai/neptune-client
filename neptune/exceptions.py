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
import warnings

from neptune import envs

STYLES = {'h1': '\033[95m',
          'h2': '\033[94m',
          'python': '\033[96m',
          'bash': '\033[95m',
          'warning': '\033[93m',
          'correct': '\033[92m',
          'fail': '\033[91m',
          'bold': '\033[1m',
          'underline': '\033[4m',
          'end': '\033[0m'}


class NeptuneException(Exception):
    pass


class NeptuneUninitializedException(NeptuneException):
    def __init__(self):
        message = """
{h1}     
----NeptuneUninitializedException---------------------------------------------------------------------------------------
{end}
You must initialize neptune-client before you create an experiment.

Looks like you forgot to add:
    {python}neptune.init(api_token='YOUR_LONG_TOKEN', project_qualified_name='WORKSPACE_NAME/PROJECT_NAME'){end}
    
before you ran:
    {python}neptune.create_experiment(){end}
    
You may also want to check the following docs pages:
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
    
{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**STYLES)
        super(NeptuneUninitializedException, self).__init__(message)


def Uninitialized():
    message = """
{warning}Uninitialized was renamed to NeptuneUninitializedException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneUninitializedException()


class FileNotFound(NeptuneException):
    def __init__(self, path):
        super(FileNotFound, self).__init__("File {} doesn't exist.".format(path))


class NotAFile(NeptuneException):
    def __init__(self, path):
        super(NotAFile, self).__init__("Path {} is not a file.".format(path))


class NotADirectory(NeptuneException):
    def __init__(self, path):
        super(NotADirectory, self).__init__("Path {} is not a directory.".format(path))


class InvalidNotebookPath(NeptuneException):
    def __init__(self, path):
        super(InvalidNotebookPath, self).__init__(
            "File {} is not a valid notebook. Should end with .ipynb.".format(path))


class InvalidChannelX(NeptuneException):
    def __init__(self, x):
        super(InvalidChannelX, self).__init__(
            "Invalid channel X-coordinate: '{}'. The sequence of X-coordinates must be strictly increasing.".format(x))


class NoChannelValue(NeptuneException):
    def __init__(self):
        super(NoChannelValue, self).__init__('No channel value provided.')


class NeptuneLibraryNotInstalledException(NeptuneException):
    def __init__(self, library):
        message = """
{h1}     
----NeptuneLibraryNotInstalledException---------------------------------------------------------------------------------
{end}
Looks like library {library} wasn't installed.

To install run:
    {bash}pip install{library}{end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/getting-started/installation/index.html
    
{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**{'library': library, **STYLES})
        super(NeptuneLibraryNotInstalledException, self).__init__(message)


def LibraryNotInstalled(library):
    message = """
{warning}LibraryNotInstalled was renamed to NeptuneLibraryNotInstalledException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneLibraryNotInstalledException(library)


class InvalidChannelValue(NeptuneException):
    def __init__(self, expected_type, actual_type):
        super(InvalidChannelValue, self).__init__(
            'Invalid channel value type. Expected: {expected}, actual: {actual}.'.format(
                expected=expected_type, actual=actual_type))


class NeptuneNoExperimentContextException(NeptuneException):
    def __init__(self):
        message = """
{h1}  
----NeptuneNoExperimentContextException---------------------------------------------------------------------------------
{end}
Neptune couldn't find an active experiment.

Looks like you forgot to run:
    {python}neptune.create_experiment(){end}
    
You may also want to check the following docs pages:
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**STYLES)
        super(NeptuneNoExperimentContextException, self).__init__(message)


def NoExperimentContext():
    message = """
{warning}NoExperimentContext was renamed to NeptuneNoExperimentContextException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneNoExperimentContextException()


class NeptuneMissingApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingApiTokenException-------------------------------------------------------------------------------------
{end}
Neptune client couldn't find your API token.

Learn how to get it in this docs page:
https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html

There are two options two add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass the token to {bold}neptune.init(){end} via {bold}api_token{end} argument:
    {python}neptune.init(api_token='YOUR_LONG_TOKEN', project_qualified_name='WORKSPACE_NAME/PROJECT_NAME'){end}

{h2}ENVIRONMENT VARIABLE{end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}=YOUR_LONG_TOKEN{end}
        
    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}=YOUR_LONG_TOKEN{end}
        
and skip the {bold}api_token{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(project_qualified_name='WORKSPACE_NAME/PROJECT_NAME'){end}
    
You may also want to check the following docs pages:
    - https://docs.neptune.ai/security-and-privacy/api-tokens/how-to-find-and-set-neptune-api-token.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**{'env_api_token': envs.API_TOKEN_ENV_NAME, **STYLES})
        super(NeptuneMissingApiTokenException, self).__init__(message)


def MissingApiToken():
    message = """
{warning}MissingApiToken was renamed to NeptuneMissingApiTokenException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneNoExperimentContextException()


class NeptuneMissingProjectQualifiedNameException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingProjectQualifiedNameException-------------------------------------------------------------------------
{end}
Neptune client couldn't find your project name.

There are two options two add it:
    - specify it in your code 
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass it to {bold}neptune.init(){end} via {bold}project_qualified_name{end} argument:
    {python}neptune.init(api_token='YOUR_LONG_TOKEN', project_qualified_name='WORKSPACE_NAME/PROJECT_NAME'){end}

{h2}ENVIRONMENT VARIABLE{end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
       {bash}export {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

    {correct}Windows{end}
    In your CMD run:
       {bash}set {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

and skip the {bold}project_qualified_name{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(api_token='YOUR_LONG_TOKEN'){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html
    
{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**{'env_project': envs.PROJECT_ENV_NAME, **STYLES})
        super(NeptuneMissingProjectQualifiedNameException, self).__init__(message)


def MissingProjectQualifiedName():
    message = """
{warning}MissingProjectQualifiedName was renamed to NeptuneMissingProjectQualifiedNameException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneMissingProjectQualifiedNameException()


class NeptuneIncorrectProjectQualifiedNameException(NeptuneException):
    def __init__(self, project_qualified_name):
        message = """
{h1}
----NeptuneIncorrectProjectQualifiedNameException-----------------------------------------------------------------------
{end}
Project qualified name {fail}{project_qualified_name}{end} you specified was incorrect.

The correct project qualified name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
It has two parts:
    - {correct}WORKSPACE{end}: which can be your username or your organization name
    - {correct}PROJECT_NAME{end}: which is the actual project name you chose 

For example, a project {correct}jakub-czakon/blog-hpo{end} parts are:
    - {correct}jakub-czakon{end}: a {underline}WORKSPACE{end} my username
    - {correct}blog-hpo{end}: a {underline}PROJECT_NAME{end} a project name
    
The URL to this project looks like this: https://ui.neptune.ai/jakub-czakon/blog-hpo 

You may also want to check the following docs pages:
    - https://docs.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Get help{end}: https://docs.neptune.ai/getting-started/getting-help.html
""".format(**{'project_qualified_name': project_qualified_name, **STYLES})
        super(NeptuneIncorrectProjectQualifiedNameException, self).__init__(message)


def IncorrectProjectQualifiedName(project_qualified_name):
    message = """
{warning}IncorrectProjectQualifiedName was renamed to NeptuneIncorrectProjectQualifiedNameException and will be removed in the future releases.{end}
""".format(**STYLES)
    warnings.warn(message)
    return NeptuneIncorrectProjectQualifiedNameException(project_qualified_name)


class InvalidNeptuneBackend(NeptuneException):
    def __init__(self, provided_backend_name):
        super(InvalidNeptuneBackend, self).__init__(
            'Unknown {} "{}". '
            'Use this environment variable to modify neptune-client behaviour at runtime, '
            'e.g. using {}=offline allows you to run your code without logging anything to Neptune'
            .format(envs.BACKEND, provided_backend_name, envs.BACKEND))


class DeprecatedApiToken(NeptuneException):
    def __init__(self, app_url):
        super(DeprecatedApiToken, self).__init__(
            "Your API token is deprecated. Please visit {} to get a new one.".format(app_url))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        super(CannotResolveHostname, self).__init__(
            "Cannot resolve hostname {}. Please contact Neptune support.".format(host))


class UnsupportedClientVersion(NeptuneException):
    def __init__(self, version, minVersion, maxVersion):
        super(UnsupportedClientVersion, self).__init__(
            "This client version ({}) is not supported. Please install neptune-client{}".format(
                version,
                "==" + str(maxVersion) if maxVersion else ">=" + str(minVersion)
            ))
