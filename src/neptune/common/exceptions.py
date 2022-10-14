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

from neptune.common.envs import API_TOKEN_ENV_NAME

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
    - set as an environment variable in your operating system.

{h2}CODE{end}
Pass the token to the {bold}init(){end} method via the {bold}api_token{end} argument:
    {python}neptune.init_run(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system:

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export {env_api_token}="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In your CMD run:
        {bash}set {env_api_token}="YOUR_API_TOKEN"{end}

and skip the {bold}api_token{end} argument of the {bold}init(){end} method:
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
