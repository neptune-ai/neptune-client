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
from neptune.new.envs import CUSTOM_RUN_ID_ENV_NAME
from neptune.new.internal.utils import replace_patch_version
from neptune.new.internal.backends.api_model import Project, Workspace
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


class MissingFieldException(NeptuneException, AttributeError, KeyError):
    """Raised when get-like action is called on `Handler`, instead of on `Attribute`."""

    def __init__(self, field_path):
        message = """
{h1}
----MissingFieldException-------------------------------------------------------
{end}
Field "{field_path}" was not found.

There are two possible reasons:
    - There is a typo in a path. Double-check your code for typos.
    - You are fetching a field that other process created, but local representation is not synchronized.
    If you are sending metadata from multiple processes at the same time, synchronize the local representation before fetching values:
        {python}run.sync(){end}

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        self._msg = message.format(
            field_path=field_path,
            **STYLES
        )
        super().__init__(self._msg)

    def __str__(self):
        # required because of overriden `__str__` in `KeyError`
        return self._msg


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

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            msg=msg,
            **STYLES
        ))


class ClientHttpError(NeptuneException):
    def __init__(self, status, response):
        self.status = status
        self.response = response
        message = """
{h1}
----ClientHttpError-----------------------------------------------------------------------
{end}
Neptune server returned status {fail}{status}{end}.

Server response was:
{fail}{response}{end}

Verify the correctness of your call or contact Neptune support.

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            status=status,
            response=response,
            **STYLES
        ))


class ExceptionWithProjectsWorkspacesListing(NeptuneException):
    def __init__(self,
                 message: str,
                 available_projects: List[Project] = (),
                 available_workspaces: List[Workspace] = (),
                 **kwargs):
        available_projects_message = """
Did you mean any of these?
{projects}
"""

        available_workspaces_message = """
You can check all of your projects on the Projects page:
{workspaces_urls}
"""

        projects_formated_list = '\n'.join(
            map(lambda project: f'    - {project.workspace}/{project.name}', available_projects)
        )

        workspaces_formated_list = '\n'.join(
            map(lambda workspace: f'    - https://app.neptune.ai/{workspace.name}/-/projects', available_workspaces)
        )

        super().__init__(message.format(
            available_projects_message=available_projects_message.format(
                projects=projects_formated_list
            ) if available_projects else '',
            available_workspaces_message=available_workspaces_message.format(
                workspaces_urls=workspaces_formated_list
            ) if available_workspaces else '',
            **STYLES,
            **kwargs
        ))


class ProjectNotFound(ExceptionWithProjectsWorkspacesListing):
    def __init__(self,
                 project_id: str,
                 available_projects: List[Project] = (),
                 available_workspaces: List[Workspace] = ()):
        message = """
{h1}
----NeptuneProjectNotFoundException------------------------------------
{end}
We couldn’t find project {fail}"{project}"{end}.
{available_projects_message}{available_workspaces_message}
You may also want to check the following docs pages:
    - https://docs.neptune.ai/administration/workspace-project-and-user-management/projects
    - https://docs.neptune.ai/getting-started/hello-world#project

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message=message,
                         available_projects=available_projects,
                         available_workspaces=available_workspaces,
                         project=project_id)


class ProjectNameCollision(ExceptionWithProjectsWorkspacesListing):
    def __init__(self,
                 project_id: str,
                 available_projects: List[Project] = ()):
        message = """
{h1}
----NeptuneProjectNameCollisionException------------------------------------
{end}
Cannot resolve project {fail}"{project}"{end}.
{available_projects_message}
You may also want to check the following docs pages:
    - https://docs.neptune.ai/administration/workspace-project-and-user-management/projects
    - https://docs.neptune.ai/getting-started/hello-world#project

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message=message,
                         available_projects=available_projects,
                         project=project_id)


class NeptuneMissingProjectNameException(ExceptionWithProjectsWorkspacesListing):
    def __init__(self,
                 available_projects: List[Project] = (),
                 available_workspaces: List[Workspace] = ()):
        message = """
{h1}
----NeptuneMissingProjectNameException----------------------------------------
{end}
Neptune client couldn't find your project name.
{available_projects_message}{available_workspaces_message}
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
    - https://docs.neptune.ai/administration/workspace-project-and-user-management/projects
    - https://docs.neptune.ai/getting-started/hello-world#project

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message=message,
                         available_projects=available_projects,
                         available_workspaces=available_workspaces,
                         env_project=envs.PROJECT_ENV_NAME)


class RunNotFound(NeptuneException):

    def __init__(self, run_id: str) -> None:
        super().__init__("Run {} not found.".format(run_id))


class RunUUIDNotFound(NeptuneException):
    def __init__(self, run_uuid: uuid.UUID):
        super().__init__("Run with UUID {} not found. Could be deleted.".format(run_uuid))


class InactiveRunException(NeptuneException):
    def __init__(self, short_id: str):
        message = """
{h1}
----InactiveRunException----------------------------------------
{end}
It seems you are trying to log (or fetch) metadata to a run that was stopped ({short_id}).
What should I do?
    - Resume the run to continue logging to it:
    https://docs.neptune.ai/how-to-guides/neptune-api/resume-run#how-to-resume-run
    - Don't invoke `stop()` on a run that you want to access. If you want to stop monitoring only, 
    you can resume a run in read-only mode:
    https://docs.neptune.ai/you-should-know/connection-modes#read-only
You may also want to check the following docs pages:
    - https://docs.neptune.ai/api-reference/run#stop
    - https://docs.neptune.ai/how-to-guides/neptune-api/resume-run#how-to-resume-run
    - https://docs.neptune.ai/you-should-know/connection-modes
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            short_id=short_id,
            **STYLES
        ))


class NeptuneMissingApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingApiTokenException-------------------------------------------
{end}
Neptune client couldn't find your API token.

You can get it here:
    - https://app.neptune.ai/get_my_api_token

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
    - https://docs.neptune.ai/getting-started/installation#authentication-neptune-api-token

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            env_api_token=envs.API_TOKEN_ENV_NAME,
            **STYLES
        ))


class NeptuneInvalidApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneInvalidApiTokenException------------------------------------------------
{end}
Provided API token is invalid.
Make sure you copied and provided your API token correctly.

You can get it or check if it is correct here:
    - https://app.neptune.ai/get_my_api_token

There are two options to add it:
    - specify it in your code 
    - set as an environment variable in your operating system.

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
    - https://docs.neptune.ai/getting-started/installation#authentication-neptune-api-token

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            env_api_token=envs.API_TOKEN_ENV_NAME,
            **STYLES
        ))


class CannotSynchronizeOfflineRunsWithoutProject(NeptuneException):
    def __init__(self):
        super().__init__("Cannot synchronize offline runs without a project.")


class NeedExistingRunForReadOnlyMode(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeedExistingRunForReadOnlyMode-----------------------------------------
{end}
Read-only mode can be used only with an existing run.

Parameter {python}run{end} of {python}neptune.init(){end} must be provided and reference
an existing run when using {python}mode="read-only"{end}.

You may also want to check the following docs pages:
    - https://docs.neptune.ai/you-should-know/connection-modes#read-only
    - https://docs.neptune.ai/api-reference/neptune#init

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class NeptuneRunResumeAndCustomIdCollision(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneRunResumeAndCustomIdCollision-----------------------------------------
{end}
It's not possible to use {python}custom_run_id{end} while resuming a run.

Parameters {python}run{end} and {python}custom_run_id{end} of {python}neptune.init(){end} are mutually exclusive.
Make sure you have no {bash}{custom_id_env}{end} environment variable set
and no value is explicitly passed to `custom_run_id` argument when you are resuming a run.

You may also want to check the following docs pages:
    - https://docs.neptune.ai/api-reference/neptune#init

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            custom_id_env=CUSTOM_RUN_ID_ENV_NAME,
            **STYLES
        ))


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
----UnsupportedClientVersion-------------------------------------------------------------
{end}
Your version of neptune-client ({current_version}) library is not supported by the Neptune server.

Please install neptune-client{required_version}

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            current_version=current_version,
            required_version=required_version,
            **STYLES
        ))


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
      See https://docs.neptune.ai/api-reference/neptune#init
      and https://requests.readthedocs.io/en/master/user/advanced/#proxies
    - Check Neptune services status: https://status.neptune.ai/

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            host=host,
            **STYLES
        ))


class SSLError(NeptuneException):
    def __init__(self):
        super().__init__(
            'SSL certificate validation failed. Set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE '
            'environment variable to accept self-signed certificates.')


class NeptuneConnectionLostException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneConnectionLostException---------------------------------------------------------
{end}
A connection to the Neptune server was lost.
If you are using asynchronous (default) connection mode Neptune will continue to locally track your metadata and will continuously try to re-establish connection with Neptune servers.
If the connection is not re-established you can upload it later using Neptune Command Line Interface:
    {bash}neptune sync -p workspace_name/project_name{end}

What should I do?
    - Check if your computer is connected to the internet.
    - If your connection is unstable you can consider working using the offline mode:
        {python}run = neptune.init(mode="offline"){end}
        
You can read in detail how it works and how to upload your data on the following doc pages:
    - https://docs.neptune.ai/you-should-know/connection-modes#offline
    - https://docs.neptune.ai/you-should-know/connection-modes#uploading-offline-data
    
You may also want to check the following docs pages:
    - https://docs.neptune.ai/you-should-know/connection-modes#connectivity-issues
    - https://docs.neptune.ai/you-should-know/connection-modes
    
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class InternalServerError(NeptuneApiException):
    def __init__(self, response):
        message = """
{h1}
----InternalServerError-----------------------------------------------------------------------
{end}
Neptune Client Library encountered an unexpected Internal Server Error.

Server response was:
{fail}{response}{end}

Please try again later or contact Neptune support.

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            response=response,
            **STYLES
        ))


class Unauthorized(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Unauthorized-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://app.neptune.ai/get_my_api_token
      
    - Verify if your the provided project name is correct.
      The correct project name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
   - Ask your organization administrator to grant you necessary privileges to the project

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class Forbidden(NeptuneApiException):
    def __init__(self):
        message = """
{h1}
----Forbidden-----------------------------------------------------------------------
{end}
You have no permission to access given resource.
    
    - Verify your API token is correct.
      See: https://app.neptune.ai/get_my_api_token
      
    - Verify if your the provided project name is correct.
      The correct project name should look like this {correct}WORKSPACE/PROJECT_NAME{end}.
      It has two parts:
          - {correct}WORKSPACE{end}: which can be your username or your organization name
          - {correct}PROJECT_NAME{end}: which is the actual project name you chose
          
   - Ask your organization administrator to grant you necessary privileges to the project

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class NeptuneOfflineModeFetchException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneOfflineModeFetchException---------------------------------------------------
{end}
It seems you are trying to fetch data from the server, while working in an offline mode.
You need to work in non-offline connection mode to fetch data from the server. 

You can set connection mode when creating a new run:
    {python}run = neptune.init(mode="async"){end}
    
You may also want to check the following docs pages:
    - https://docs.neptune.ai/you-should-know/connection-modes
    
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class OperationNotSupported(NeptuneException):
    def __init__(self, message: str):
        super().__init__(f'Operation not supported: {message}')


class NeptuneLegacyProjectException(NeptuneException):
    def __init__(self, project: str):
        message = """
{h1}
----NeptuneLegacyProjectException---------------------------------------------------------
{end}
Your project "{project}" has not been migrated to the new structure yet.
Unfortunately neptune.new Python API is incompatible with projects using old structure,
please use legacy neptune Python API.
Don't worry - we are working hard on migrating all the projects and you will be able to use the neptune.new API soon. 

You can find documentation for legacy neptune Python API here:
    - https://docs-legacy.neptune.ai/index.html
    
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            project=project,
            **STYLES
        ))


class NeptuneUninitializedException(NeptuneException):
    def __init__(self):
        message = """
{h1}     
----NeptuneUninitializedException----------------------------------------------------
{end}
You must initialize neptune-client before you access `get_last_run`.

Looks like you forgot to add:
    {python}neptune.init(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

before you ran:
    {python}neptune.get_last_run(){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/api-reference/neptune#get_last_run

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


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
    - https://docs.neptune.ai/integrations-and-supported-tools/intro
    
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(
            framework=framework,
            **STYLES
        ))


class NeptuneStorageLimitException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneStorageLimitException---------------------------------------------------------------------------------------
{end}
You exceeded storage limit for workspace. It's not possible to upload new data, but you can still fetch and delete data.
If you are using asynchronous (default) connection mode Neptune automatically switched to an offline mode
and your data is being stored safely on the disk. You can upload it later using Neptune Command Line Interface:
    {bash}neptune sync -p project_name{end}
What should I do?
    - Go to your projects and remove runs or model metadata you don't need
    - ... or update your subscription plan here: https://app.neptune.ai/-/subscription
You may also want to check the following docs pages:
    - https://docs.neptune.ai/advanced-user-guides/connection-modes
{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class FetchAttributeNotFoundException(MetadataInconsistency):
    def __init__(self, attribute_path: str):
        message = """
{h1}
----MetadataInconsistency----------------------------------------------------------------------
{end}
Field {python}{attribute_path}{end} was not found.

Remember that in the asynchronous (default) connection mode data is synchronized
with the Neptune servers in the background and may have not reached
it yet before it's fetched. Before fetching the data you can force
wait for all the requests sent by invoking:

    {python}run.wait(){end}
    
Remember that each use of {python}wait{end} introduces a delay in code execution.

You may also want to check the following docs pages:
    - https://docs.neptune.ai/you-should-know/connection-modes

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help.html
"""
        super().__init__(message.format(
            attribute_path=attribute_path,
            **STYLES
        ))


class PlotlyIncompatibilityException(Exception):
    def __init__(self, matplotlib_version, plotly_version):
        super().__init__(
            "Unable to convert plotly figure to matplotlib format. "
            "Your matplotlib ({}) and plotlib ({}) versions are not compatible. "
            "See https://stackoverflow.com/q/63120058 for details. "
            "Downgrade matplotlib to version 3.2 or use as_image to log static chart.".format(
                matplotlib_version,
                plotly_version))


class NeptunePossibleLegacyUsageException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptunePossibleLegacyUsageException----------------------------------------------------------------
{end}
It seems you are trying to use legacy API, but imported the new one.

Simply update your import statement to:
    {python}import neptune{end}

You may want to check the Legacy API docs:
    - https://docs-legacy.neptune.ai

If you want to update your code with the new API we prepared a handy migration guide:
    - https://docs.neptune.ai/migration-guide

You can read more about neptune.new in the release blog post:
    - https://neptune.ai/blog/neptune-new

You may also want to check the following docs pages:
    - https://docs-legacy.neptune.ai/getting-started/integrate-neptune-into-your-codebase.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))


class NeptuneLegacyIncompatibilityException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneLegacyIncompatibilityException----------------------------------------
{end}
It seems you are passing the legacy Experiment object, when a Run object is expected.

What can I do?
    - Updating your code to the new Python API requires few changes, but to help you with this process we prepared a handy migration guide:
    https://docs.neptune.ai/migration-guide
    - You can read more about neptune.new in the release blog post:
    https://neptune.ai/blog/neptune-new

{correct}Need help?{end}-> https://docs.neptune.ai/getting-started/getting-help
"""
        super().__init__(message.format(**STYLES))
