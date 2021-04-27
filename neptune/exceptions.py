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
import platform

from neptune import envs

UNIX_STYLES = {'h1': '\033[95m',
               'h2': '\033[94m',
               'blue': '\033[94m',
               'python': '\033[96m',
               'bash': '\033[95m',
               'warning': '\033[93m',
               'correct': '\033[92m',
               'fail': '\033[91m',
               'bold': '\033[1m',
               'underline': '\033[4m',
               'end': '\033[0m'}

WINDOWS_STYLES = {'h1': '',
                  'h2': '',
                  'python': '',
                  'bash': '',
                  'warning': '',
                  'correct': '',
                  'fail': '',
                  'bold': '',
                  'underline': '',
                  'end': ''}

EMPTY_STYLES = {'h1': '',
                'h2': '',
                'python': '',
                'bash': '',
                'warning': '',
                'correct': '',
                'fail': '',
                'bold': '',
                'underline': '',
                'end': ''}

if platform.system() in ['Linux', 'Darwin']:
    STYLES = UNIX_STYLES
elif platform.system() == 'Windows':
    STYLES = WINDOWS_STYLES
else:
    STYLES = EMPTY_STYLES


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
    {python}neptune.init(project_qualified_name='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}
    
before you ran:
    {python}neptune.create_experiment(){end}
    
You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/getting-started/quick-starts/log_first_experiment.html
    
{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
""".format(**STYLES)
        super(NeptuneUninitializedException, self).__init__(message)


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
    {bash}pip install {library}{end}

You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/getting-started/installation/index.html
    
{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'library': library}.items()) + list(STYLES.items()))
        super(NeptuneLibraryNotInstalledException, self).__init__(message.format(**inputs))


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
    - https://docs-legacy.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
"""
        super(NeptuneNoExperimentContextException, self).__init__(message.format(**STYLES))


class NeptuneMissingApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingApiTokenException-------------------------------------------------------------------------------------
{end}
Neptune client couldn't find your API token.

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
        inputs = dict(list({'env_api_token': envs.API_TOKEN_ENV_NAME}.items()) + list(STYLES.items()))
        super(NeptuneMissingApiTokenException, self).__init__(message.format(**inputs))


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
    {python}neptune.init(project_qualified_name='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end}
or export or set an environment variable depending on your operating system: 

    {correct}Linux/Unix{end}
    In your terminal run:
       {bash}export {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

    {correct}Windows{end}
    In your CMD run:
       {bash}set {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

and skip the {bold}project_qualified_name{end} argument of {bold}neptune.init(){end}: 
    {python}neptune.init(api_token='YOUR_API_TOKEN'){end}

You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs-legacy.neptune.ai/getting-started/quick-starts/log_first_experiment.html
    
{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'env_project': envs.PROJECT_ENV_NAME}.items()) + list(STYLES.items()))
        super(NeptuneMissingProjectQualifiedNameException, self).__init__(message.format(**inputs))


class NeptuneIncorrectProjectQualifiedNameException(NeptuneException):
    def __init__(self, project_qualified_name):
        message = """
{h1}
----NeptuneIncorrectProjectQualifiedNameException-----------------------------------------------------------------------
{end}
Project qualified name {fail}"{project_qualified_name}"{end} you specified was incorrect.

The correct project qualified name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
It has two parts:
    - {correct}WORKSPACE{end}: which can be your username or your organization name
    - {correct}PROJECT_NAME{end}: which is the actual project name you chose 

For example, a project {correct}neptune-ai/credit-default-prediction{end} parts are:
    - {correct}neptune-ai{end}: {underline}WORKSPACE{end} our company organization name
    - {correct}credit-default-prediction{end}: {underline}PROJECT_NAME{end} a project name
    
The URL to this project looks like this: https://app.neptune.ai/neptune-ai/credit-default-prediction

You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/workspace-project-and-user-management/index.html
    - https://docs-legacy.neptune.ai/getting-started/quick-starts/log_first_experiment.html

{correct}Need help?{end}-> https://docs-legacy.neptune.ai/getting-started/getting-help.html
"""
        inputs = dict(list({'project_qualified_name': project_qualified_name}.items()) + list(STYLES.items()))
        super(NeptuneIncorrectProjectQualifiedNameException, self).__init__(message.format(**inputs))


class InvalidNeptuneBackend(NeptuneException):
    def __init__(self, provided_backend_name):
        super(InvalidNeptuneBackend, self).__init__(
            'Unknown {} "{}". '
            'Use this environment variable to modify neptune-client behaviour at runtime, '
            'e.g. using {}=offline allows you to run your code without logging anything to Neptune'
            ''.format(envs.BACKEND, provided_backend_name, envs.BACKEND))


class DeprecatedApiToken(NeptuneException):
    def __init__(self, app_url):
        super(DeprecatedApiToken, self).__init__(
            "Your API token is deprecated. Please visit {} to get a new one.".format(app_url))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        message = """
{h1}
----CannotResolveHostname-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to resolve hostname {host}.

What should I do?
    - Check if your computer is connected to the internet.
    - Check if your computer should use any proxy to access internet.
      If so, you may want to use {python}proxies{end} parameter of {python}neptune.init(){end} function.
      See https://docs-legacy.neptune.ai/api-reference/neptune/index.html#neptune.init
      and https://requests.readthedocs.io/en/master/user/advanced/#proxies
"""
        inputs = dict(list({'host': host}.items()) + list(STYLES.items()))
        super(CannotResolveHostname, self).__init__(message.format(**inputs))


class UnsupportedClientVersion(NeptuneException):
    def __init__(self, version, minVersion, maxVersion):
        super(UnsupportedClientVersion, self).__init__(
            "This client version ({}) is not supported. Please install neptune-client{}".format(
                version,
                "==" + str(maxVersion) if maxVersion else ">=" + str(minVersion)
            ))


class UnsupportedInAlphaException(NeptuneException):
    """Raised for operations which was available in old client,
    but aren't supported in alpha version"""


class DownloadSourcesException(UnsupportedInAlphaException):
    message = """
{h1}
----DownloadSourcesException-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to download single file from sources.

Why am I seeing this?
    Your project "{project}" has been migrated to new structure.
    Old version of `neptune-api` is not supporting downloading particular source files.
    We recommend you to use new version of api: `neptune.new`.
    {correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help

If you don't want to adapt your code to new api yet,
you can use `download_sources` with `path` parameter set to None.
"""

    def __init__(self, experiment):
        assert self.message is not None
        super().__init__(
            self.message.format(
                project=experiment._project.internal_id,
                **STYLES,
            )
        )


class DownloadArtifactsUnsupportedException(UnsupportedInAlphaException):
    message = """
{h1}
----DownloadArtifactsUnsupportedException-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to download artifacts.
Function `download_artifacts` is deprecated.

Why am I seeing this?
    Your project "{project}" has been migrated to new structure.
    Old version of `neptune-api` is not supporting downloading artifact directories.
    We recommend you to use new version of api: `neptune.new`.
    {correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help

If you don't want to adapt your code to new api yet,
you can use `download_artifact` and download files one by one.
"""

    def __init__(self, experiment):
        assert self.message is not None
        super().__init__(
            self.message.format(
                project=experiment._project.internal_id,
                **STYLES,
            )
        )


class DownloadArtifactUnsupportedException(UnsupportedInAlphaException):
    message = """
{h1}
----DownloadArtifactUnsupportedException-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to download attribute: "{artifact_path}".
It's not present in experiment {experiment} or is a directory.

Why am I seeing this?
    Your project "{project}" has been migrated to new structure.
    Old version of `neptune-api` is not supporting downloading whole artifact directories.
    We recommend you to use new version of api: `neptune.new`.
    {correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help

If you don't want to adapt your code to new api yet:
    - Make sure that artifact "{artifact_path}" is present in experiment "{experiment}".
    - Make sure that you're addressing artifact which is file, not whole directory.
"""

    def __init__(self, artifact_path, experiment):
        assert self.message is not None
        super().__init__(
            self.message.format(
                artifact_path=artifact_path,
                experiment=experiment.id,
                project=experiment._project.internal_id,
                **STYLES,
            )
        )


class DeleteArtifactUnsupportedInAlphaException(UnsupportedInAlphaException):
    message = """
{h1}
----DeleteArtifactUnsupportedInAlphaException-----------------------------------------------------------------------
{end}
Neptune Client Library was not able to delete attribute: "{artifact_path}".
It's not present in experiment {experiment} or is a directory.

Why am I seeing this?
    Your project "{project}" has been migrated to new structure.
    Old version of `neptune-api` is not supporting deleting whole artifact directories.
    We recommend you to use new version of api: `neptune.new`.
    {correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help

If you don't want to adapt your code to new api yet:
    - Make sure that artifact "{artifact_path}" is present in experiment "{experiment}".
    - Make sure that you're addressing artifact which is file, not whole directory.
"""

    def __init__(self, artifact_path, experiment):
        assert self.message is not None
        super().__init__(
            self.message.format(
                artifact_path=artifact_path,
                experiment=experiment.id,
                project=experiment._project.internal_id,
                **STYLES,
            )
        )


class ProjectMigratedToNewStructure(NeptuneException):
    pass
