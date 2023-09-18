#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from neptune.common.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)

UNIX_STYLES = {
    "h1": "\033[95m",
    "h2": "\033[94m",
    "blue": "\033[94m",
    "python": "\033[96m",
    "bash": "\033[95m",
    "warning": "\033[93m",
    "correct": "\033[92m",
    "fail": "\033[91m",
    "bold": "\033[1m",
    "underline": "\033[4m",
    "end": "\033[0m",
}

WINDOWS_STYLES = {
    "h1": "",
    "h2": "",
    "python": "",
    "bash": "",
    "warning": "",
    "correct": "",
    "fail": "",
    "bold": "",
    "underline": "",
    "end": "",
}

EMPTY_STYLES = {
    "h1": "",
    "h2": "",
    "python": "",
    "bash": "",
    "warning": "",
    "correct": "",
    "fail": "",
    "bold": "",
    "underline": "",
    "end": "",
}

if platform.system() in ["Linux", "Darwin"]:
    STYLES = UNIX_STYLES
elif platform.system() == "Windows":
    STYLES = WINDOWS_STYLES
else:
    STYLES = EMPTY_STYLES


class NeptuneException(Exception):
    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and str(self).__eq__(str(other))
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), str(self)))


class NeptuneInvalidApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneInvalidApiTokenException------------------------------------------------
{end}
The provided API token is invalid.
Make sure you copied and provided your API token correctly.

You can get it or check if it is correct here:
    - https://app.neptune.ai/get_my_api_token

There are two options to add it:
    - specify it in your code
    - set it as an environment variable in your operating system.

{h2}CODE{end}
Pass the token to the {bold}init_run(){end} function via the {bold}api_token{end} argument:
    {python}neptune.init_run(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system:

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}="YOUR_API_TOKEN"{end}

and skip the {bold}api_token{end} argument of the {bold}init_run(){end} function:
    {python}neptune.init_run(project='WORKSPACE_NAME/PROJECT_NAME'){end}

You may also want to check the following docs page:
    - https://docs.neptune.ai/setup/setting_api_token/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(env_api_token=API_TOKEN_ENV_NAME, **STYLES))


class UploadedFileChanged(NeptuneException):
    def __init__(self, filename: str):
        super().__init__("File {} changed during upload, restarting upload.".format(filename))


class InternalClientError(NeptuneException):
    def __init__(self, msg: str):
        message = """
{h1}
----InternalClientError-----------------------------------------------------------------------
{end}
The Neptune client library encountered an unexpected internal error:
{msg}

Please contact Neptune support.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(msg=msg, **STYLES))


class ClientHttpError(NeptuneException):
    def __init__(self, status, response):
        self.status = status
        self.response = response
        message = """
{h1}
----ClientHttpError-----------------------------------------------------------------------
{end}
The Neptune server returned the status {fail}{status}{end}.

The server response was:
{fail}{response}{end}

Verify the correctness of your call or contact Neptune support.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(status=status, response=response, **STYLES))


class NeptuneApiException(NeptuneException):
    pass


class Forbidden(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Forbidden-----------------------------------------------------------------------
{end}
You don't have permission to access the given resource.

    - Verify that your API token is correct.
      See: https://app.neptune.ai/get_my_api_token

    - Verify that the provided project name is correct.
      The correct project name should look like this: {correct}WORKSPACE_NAME/PROJECT_NAME{end}
      It has two parts:
          - {correct}WORKSPACE_NAME{end}: can be your username or your organization name
          - {correct}PROJECT_NAME{end}: the name specified for the project

   - Ask your organization administrator to grant you the necessary privileges to the project.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class Unauthorized(NeptuneApiException):
    def __init__(self, msg=None):
        default_message = """
{h1}
----Unauthorized-----------------------------------------------------------------------
{end}
You don't have permission to access the given resource.

    - Verify that your API token is correct.
      See: https://app.neptune.ai/get_my_api_token

    - Verify that the provided project name is correct.
      The correct project name should look like this: {correct}WORKSPACE_NAME/PROJECT_NAME{end}
      It has two parts:
          - {correct}WORKSPACE_NAME{end}: can be your username or your organization name
          - {correct}PROJECT_NAME{end}: the name specified for the project

   - Ask your organization administrator to grant you the necessary privileges to the project.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        message = msg if msg is not None else default_message
        super().__init__(message.format(**STYLES))


class NeptuneAuthTokenExpired(Unauthorized):
    def __init__(self):
        super().__init__("Authorization token expired")


class InternalServerError(NeptuneApiException):
    def __init__(self, response):
        message = """
{h1}
----InternalServerError-----------------------------------------------------------------------
{end}
The Neptune client library encountered an unexpected internal server error.

The server response was:
{fail}{response}{end}

Please try again later or contact Neptune support.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(response=response, **STYLES))


class NeptuneConnectionLostException(NeptuneException):
    def __init__(self, cause: Exception):
        self.cause = cause
        message = """
{h1}
----NeptuneConnectionLostException---------------------------------------------------------
{end}
The connection to the Neptune server was lost.
If you are using the asynchronous (default) connection mode, Neptune continues to locally track your metadata and continuously tries to re-establish a connection to the Neptune servers.
If the connection is not re-established, you can upload your data later with the Neptune Command Line Interface tool:
    {bash}neptune sync -p workspace_name/project_name{end}

What should I do?
    - Check if your computer is connected to the internet.
    - If your connection is unstable, consider working in offline mode:
        {python}run = neptune.init_run(mode="offline"){end}

You can find detailed instructions on the following doc pages:
    - https://docs.neptune.ai/api/connection_modes/#offline-mode
    - https://docs.neptune.ai/api/neptune_sync/

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes/#connectivity-issues

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(message.format(**STYLES))


class NeptuneSSLVerificationError(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneSSLVerificationError-----------------------------------------------------------------------
{end}

The Neptune client was unable to verify your SSL Certificate.

{bold}What could have gone wrong?{end}
    - You are behind a proxy that inspects traffic to Neptune servers.
        - Contact your network administrator
    - The SSL/TLS certificate of your on-premises installation is not recognized due to a custom Certificate Authority (CA).
        - To check, run the following command in your terminal:
            {bash}curl https://<your_domain>/api/backend/echo {end}
        - Where <your_domain> is the address that you use to access Neptune app, such as abc.com
        - Contact your network administrator if you get the following output:
            {fail}"curl: (60) server certificate verification failed..."{end}
    - Your machine software is outdated.
        - Minimal OS requirements:
            - Windows >= XP SP3
            - macOS >= 10.12.1
            - Ubuntu >= 12.04
            - Debian >= 8

{bold}What can I do?{end}
You can manually configure Neptune to skip all SSL checks. To do that,
set the NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE environment variable to 'TRUE'.
{bold}Note: This might mean that your connection is less secure{end}.

Linux/Unix
In your terminal run:
    {bash}export NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE='TRUE'{end}

Windows
In your terminal run:
    {bash}set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE='TRUE'{end}

Jupyter notebook
In your code cell:
    {bash}%env NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE='TRUE'{end}

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/environment_variables/#neptune_allow_self_signed_certificate


{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(message.format(**STYLES))


class FileNotFound(NeptuneException):
    def __init__(self, path):
        super(FileNotFound, self).__init__("File {} doesn't exist.".format(path))


class InvalidNotebookPath(NeptuneException):
    def __init__(self, path):
        super(InvalidNotebookPath, self).__init__(
            "File {} is not a valid notebook. Should end with .ipynb.".format(path)
        )


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
        super(NeptuneIncorrectProjectQualifiedNameException, self).__init__(
            message.format(project_qualified_name=project_qualified_name, **STYLES)
        )


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
        super(NeptuneMissingProjectQualifiedNameException, self).__init__(
            message.format(env_project=PROJECT_ENV_NAME, **STYLES)
        )


class NotAFile(NeptuneException):
    def __init__(self, path):
        super(NotAFile, self).__init__("Path {} is not a file.".format(path))


class NotADirectory(NeptuneException):
    def __init__(self, path):
        super(NotADirectory, self).__init__("Path {} is not a directory.".format(path))


class WritingToArchivedProjectException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----WritingToArchivedProjectException-----------------------------------------------------------------------
{end}
You're trying to write to a project that was archived.

Set the project as active again or use mode="read-only" at initialization to fetch metadata from it.

{correct}Need help?{end}-> https://docs.neptune.ai/help/error_writing_to_archived_project/
"""
        super(WritingToArchivedProjectException, self).__init__(message.format(**STYLES))
