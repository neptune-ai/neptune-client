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
__all__ = [
    "InternalClientError",
    "NeptuneException",
    "NeptuneInvalidApiTokenException",
    "NeptuneApiException",
    "MetadataInconsistency",
    "MissingFieldException",
    "TypeDoesNotSupportAttributeException",
    "MalformedOperation",
    "FileNotFound",
    "FileUploadError",
    "FileSetUploadError",
    "ClientHttpError",
    "MetadataContainerNotFound",
    "ProjectNotFound",
    "RunNotFound",
    "ModelNotFound",
    "ModelVersionNotFound",
    "ExceptionWithProjectsWorkspacesListing",
    "ContainerUUIDNotFound",
    "RunUUIDNotFound",
    "ProjectNotFoundWithSuggestions",
    "AmbiguousProjectName",
    "NeptuneMissingProjectNameException",
    "InactiveContainerException",
    "InactiveRunException",
    "InactiveModelException",
    "InactiveModelVersionException",
    "InactiveProjectException",
    "NeptuneMissingApiTokenException",
    "CannotSynchronizeOfflineRunsWithoutProject",
    "NeedExistingExperimentForReadOnlyMode",
    "NeedExistingRunForReadOnlyMode",
    "NeedExistingModelForReadOnlyMode",
    "NeedExistingModelVersionForReadOnlyMode",
    "NeptuneParametersCollision",
    "NeptuneWrongInitParametersException",
    "NeptuneRunResumeAndCustomIdCollision",
    "NeptuneClientUpgradeRequiredError",
    "NeptuneMissingRequiredInitParameter",
    "CannotResolveHostname",
    "NeptuneSSLVerificationError",
    "NeptuneConnectionLostException",
    "InternalServerError",
    "Unauthorized",
    "Forbidden",
    "NeptuneOfflineModeException",
    "NeptuneOfflineModeFetchException",
    "NeptuneOfflineModeChangeStageException",
    "NeptuneProtectedPathException",
    "NeptuneCannotChangeStageManually",
    "OperationNotSupported",
    "NeptuneLegacyProjectException",
    "NeptuneUninitializedException",
    "NeptuneIntegrationNotInstalledException",
    "NeptuneLimitExceedException",
    "NeptuneFieldCountLimitExceedException",
    "NeptuneStorageLimitException",
    "FetchAttributeNotFoundException",
    "ArtifactNotFoundException",
    "PlotlyIncompatibilityException",
    "NeptunePossibleLegacyUsageException",
    "NeptuneLegacyIncompatibilityException",
    "NeptuneUnhandledArtifactSchemeException",
    "NeptuneUnhandledArtifactTypeException",
    "NeptuneLocalStorageAccessException",
    "NeptuneRemoteStorageCredentialsException",
    "NeptuneRemoteStorageAccessException",
    "ArtifactUploadingError",
    "NeptuneUnsupportedArtifactFunctionalityException",
    "NeptuneEmptyLocationException",
    "NeptuneFeatureNotAvailableException",
    "NeptuneObjectCreationConflict",
    "NeptuneModelKeyAlreadyExistsError",
    "NeptuneSynchronizationAlreadyStoppedException",
    "StreamAlreadyUsedException",
]

from typing import (
    List,
    Optional,
    Union,
)
from urllib.parse import urlparse

from packaging.version import Version

from neptune.common.envs import API_TOKEN_ENV_NAME

# Backward compatibility import
from neptune.common.exceptions import (
    STYLES,
    ClientHttpError,
    Forbidden,
    InternalClientError,
    InternalServerError,
    NeptuneApiException,
    NeptuneConnectionLostException,
    NeptuneException,
    NeptuneInvalidApiTokenException,
    NeptuneSSLVerificationError,
    Unauthorized,
)
from neptune.new import envs
from neptune.new.envs import CUSTOM_RUN_ID_ENV_NAME
from neptune.new.internal.backends.api_model import (
    Project,
    Workspace,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import QualifiedName
from neptune.new.internal.utils import replace_patch_version


class MetadataInconsistency(NeptuneException):
    pass


class MissingFieldException(NeptuneException, AttributeError, KeyError):
    """Raised when get-like action is called on `Handler`, instead of on `Attribute`."""

    def __init__(self, field_path):
        message = """
{h1}
----MissingFieldException-------------------------------------------------------
{end}
The field "{field_path}" was not found.

There are two possible reasons:
    - There is a typo in a path. Double-check your code for typos.
    - You are fetching a field that another process created, but the local representation is not synchronized.
    If you are sending metadata from multiple processes at the same time, synchronize the local representation before fetching values:
        {python}run.sync(){end}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        self._msg = message.format(field_path=field_path, **STYLES)
        super().__init__(self._msg)

    def __str__(self):
        # required because of overriden `__str__` in `KeyError`
        return self._msg


class TypeDoesNotSupportAttributeException(NeptuneException, AttributeError):
    def __init__(self, type_, attribute):
        message = """
{h1}
----TypeDoesNotSupportAttributeException----------------------------------------
{end}
{type} has no attribute {attribute}.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        self._msg = message.format(type=type_, attribute=attribute, **STYLES)
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


class MetadataContainerNotFound(NeptuneException):
    container_id: str
    container_type: ContainerType

    def __init__(self, container_id: str, container_type: Optional[ContainerType]):
        self.container_id = container_id
        self.container_type = container_type
        container_type_str = container_type.value.capitalize() if container_type else "object"
        super().__init__("{} {} not found.".format(container_type_str, container_id))

    @classmethod
    def of_container_type(cls, container_type: Optional[ContainerType], container_id: str):
        if container_type is None:
            return MetadataContainerNotFound(container_id=container_id, container_type=None)
        elif container_type == ContainerType.PROJECT:
            return ProjectNotFound(project_id=container_id)
        elif container_type == ContainerType.RUN:
            return RunNotFound(run_id=container_id)
        elif container_type == ContainerType.MODEL:
            return ModelNotFound(model_id=container_id)
        elif container_type == ContainerType.MODEL_VERSION:
            return ModelVersionNotFound(model_version_id=container_id)
        else:
            raise InternalClientError(f"Unexpected ContainerType: {container_type}")


class ProjectNotFound(MetadataContainerNotFound):
    def __init__(self, project_id: str):
        super().__init__(container_id=project_id, container_type=ContainerType.PROJECT)


class RunNotFound(MetadataContainerNotFound):
    def __init__(self, run_id: str):
        super().__init__(container_id=run_id, container_type=ContainerType.RUN)


class ModelNotFound(MetadataContainerNotFound):
    def __init__(self, model_id: str):
        super().__init__(container_id=model_id, container_type=ContainerType.MODEL)


class ModelVersionNotFound(MetadataContainerNotFound):
    def __init__(self, model_version_id: str):
        super().__init__(container_id=model_version_id, container_type=ContainerType.MODEL_VERSION)


class ExceptionWithProjectsWorkspacesListing(NeptuneException):
    def __init__(
        self,
        message: str,
        available_projects: List[Project] = (),
        available_workspaces: List[Workspace] = (),
        **kwargs,
    ):
        available_projects_message = """
Did you mean any of these?
{projects}
"""

        available_workspaces_message = """
You can check all of your projects on the Projects page:
{workspaces_urls}
"""

        projects_formated_list = "\n".join(
            map(
                lambda project: f"    - {project.workspace}/{project.name}",
                available_projects,
            )
        )

        workspaces_formated_list = "\n".join(
            map(
                lambda workspace: f"    - https://app.neptune.ai/{workspace.name}/-/projects",
                available_workspaces,
            )
        )

        super().__init__(
            message.format(
                available_projects_message=available_projects_message.format(projects=projects_formated_list)
                if available_projects
                else "",
                available_workspaces_message=available_workspaces_message.format(
                    workspaces_urls=workspaces_formated_list
                )
                if available_workspaces
                else "",
                **STYLES,
                **kwargs,
            )
        )


class ContainerUUIDNotFound(NeptuneException):
    container_id: str
    container_type: ContainerType

    def __init__(self, container_id: str, container_type: ContainerType):
        self.container_id = container_id
        self.container_type = container_type
        super().__init__(
            "{} with ID {} not found. It may have been deleted. "
            "You can use the 'neptune clear' command to delete junk objects from local storage.".format(
                container_type.value.capitalize(), container_id
            )
        )


# for backward compatibility
RunUUIDNotFound = ContainerUUIDNotFound


class ProjectNotFoundWithSuggestions(ExceptionWithProjectsWorkspacesListing, ProjectNotFound):
    def __init__(
        self,
        project_id: QualifiedName,
        available_projects: List[Project] = (),
        available_workspaces: List[Workspace] = (),
    ):
        message = """
{h1}
----NeptuneProjectNotFoundException------------------------------------
{end}
We couldn't find project {fail}"{project}"{end}.
{available_projects_message}{available_workspaces_message}
You may want to check the following docs page:
    - https://docs.neptune.ai/setup/creating_project/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message=message,
            available_projects=available_projects,
            available_workspaces=available_workspaces,
            project=project_id,
        )


class AmbiguousProjectName(ExceptionWithProjectsWorkspacesListing):
    def __init__(self, project_id: str, available_projects: List[Project] = ()):
        message = """
{h1}
----NeptuneProjectNameCollisionException------------------------------------
{end}
Cannot resolve project {fail}"{project}"{end}. Name is ambiguous.
{available_projects_message}
You may also want to check the following docs pages:
    - https://docs.neptune.ai/setup/creating_project/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message=message, available_projects=available_projects, project=project_id)


class NeptuneMissingProjectNameException(ExceptionWithProjectsWorkspacesListing):
    def __init__(
        self,
        available_projects: List[Project] = (),
        available_workspaces: List[Workspace] = (),
    ):
        message = """
{h1}
----NeptuneMissingProjectNameException----------------------------------------
{end}
The Neptune client couldn't find your project name.
{available_projects_message}{available_workspaces_message}
There are two options two add it:
    - specify it in your code
    - set an environment variable in your operating system.

{h2}CODE{end}
Pass it to the {bold}init(){end} method via the {bold}project{end} argument:
    {python}neptune.init_run(project='WORKSPACE_NAME/PROJECT_NAME'){end}

{h2}ENVIRONMENT VARIABLE{end}
or export or set an environment variable depending on your operating system:

    {correct}Linux/Unix{end}
    In your terminal run:
       {bash}export {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

    {correct}Windows{end}
    In your CMD run:
       {bash}set {env_project}=WORKSPACE_NAME/PROJECT_NAME{end}

and skip the {bold}project{end} argument of the {bold}init(){end} method:
    {python}neptune.init_run(){end}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/setup/creating_project/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message=message,
            available_projects=available_projects,
            available_workspaces=available_workspaces,
            env_project=envs.PROJECT_ENV_NAME,
        )


class InactiveContainerException(NeptuneException):
    resume_info: str

    def __init__(self, container_type: ContainerType, label: str):
        message = """
{h1}
----{cls}----------------------------------------
{end}
It seems you are trying to log metadata to (or fetch it from) a {container_type} that was stopped ({label}).

Here's what you can do:{resume_info}

You may also want to check the following docs pages:
    - https://docs.neptune.ai/logging/to_existing_object/
    - https://docs.neptune.ai/usage/querying_metadata/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message.format(
                cls=self.__class__.__name__,
                label=label,
                container_type=container_type.value,
                resume_info=self.resume_info,
                **STYLES,
            )
        )


class InactiveRunException(InactiveContainerException):
    resume_info = """
    - Resume the run to continue logging to it:
    https://docs.neptune.ai/logging/to_existing_object/
    - Don't invoke `stop()` on a run that you want to access. If you want to stop monitoring only,
    you can resume a run in read-only mode:
    https://docs.neptune.ai/api/connection_modes/#read-only-mode"""

    def __init__(self, label: str):
        super().__init__(label=label, container_type=ContainerType.RUN)


class InactiveModelException(InactiveContainerException):
    resume_info = """
    - Resume the model to continue logging to it:
    https://docs.neptune.ai/api/neptune/#init_model
    - Don't invoke `stop()` on a model that you want to access. If you want to stop monitoring only,
    you can resume a model in read-only mode:
    https://docs.neptune.ai/api/connection_modes/#read-only-mode"""

    def __init__(self, label: str):
        super().__init__(label=label, container_type=ContainerType.MODEL)


class InactiveModelVersionException(InactiveContainerException):
    resume_info = """
    - Resume the model version to continue logging to it:
    https://docs.neptune.ai/api/neptune/#init_model_version
    - Don't invoke `stop()` on a model version that you want to access. If you want to stop monitoring only,
    you can resume a model version in read-only mode:
    https://docs.neptune.ai/api/connection_modes/#read-only-mode"""

    def __init__(self, label: str):
        super().__init__(label=label, container_type=ContainerType.MODEL_VERSION)


class InactiveProjectException(InactiveContainerException):
    resume_info = """
    - Resume the connection to the project to continue logging to it:
    https://docs.neptune.ai/api/neptune/#init_project
    - Don't invoke `stop()` on a project that you want to access."""

    def __init__(self, label: str):
        super().__init__(label=label, container_type=ContainerType.PROJECT)


class NeptuneMissingApiTokenException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneMissingApiTokenException-------------------------------------------
{end}
The Neptune client couldn't find your API token.

You can get it here:
    - https://app.neptune.ai/get_my_api_token

There are two options to add it:
    - specify it in your code
    - set an environment variable in your operating system.

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

You may also want to check the following docs pages:
    - https://docs.neptune.ai/setup/setting_api_token/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(env_api_token=API_TOKEN_ENV_NAME, **STYLES))


class CannotSynchronizeOfflineRunsWithoutProject(NeptuneException):
    def __init__(self):
        super().__init__("Cannot synchronize offline runs without a project.")


class NeedExistingExperimentForReadOnlyMode(NeptuneException):
    container_type: ContainerType
    callback_name: str

    def __init__(self, container_type: ContainerType, callback_name: str):
        message = """
{h1}
----{class_name}-----------------------------------------
{end}
Read-only mode can be used only with an existing {container_type}.

The {python}{container_type}{end} parameter of {python}{callback_name}{end} must be provided and reference
an existing run when using {python}mode="read-only"{end}.

You may also want to check the following docs pages:
    - https://docs.neptune.ai/logging/to_existing_object/
    - https://docs.neptune.ai/api/connection_modes/#read-only-mode

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        self.container_type = container_type
        self.callback_name = callback_name
        super().__init__(
            message.format(
                class_name=type(self).__name__,
                container_type=self.container_type.value,
                callback_name=self.callback_name,
                **STYLES,
            )
        )


class NeedExistingRunForReadOnlyMode(NeedExistingExperimentForReadOnlyMode):
    def __init__(self):
        super().__init__(container_type=ContainerType.RUN, callback_name="neptune.init_run")


class NeedExistingModelForReadOnlyMode(NeedExistingExperimentForReadOnlyMode):
    def __init__(self):
        super().__init__(container_type=ContainerType.MODEL, callback_name="neptune.init_model")


class NeedExistingModelVersionForReadOnlyMode(NeedExistingExperimentForReadOnlyMode):
    def __init__(self):
        super().__init__(
            container_type=ContainerType.MODEL_VERSION,
            callback_name="neptune.init_model_version",
        )


class NeptuneParametersCollision(NeptuneException):
    def __init__(self, parameter1, parameter2, method_name):
        self.parameter1 = parameter1
        self.parameter2 = parameter2
        self.method_name = method_name
        message = """
{h1}
----NeptuneParametersCollision-----------------------------------------
{end}
The {python}{parameter1}{end} and {python}{parameter2}{end} parameters of the {python}{method_name}(){end} method are mutually exclusive.

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/universal/#initialization-methods

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(
            message.format(
                parameter1=parameter1,
                parameter2=parameter2,
                method_name=method_name,
                **STYLES,
            )
        )


class NeptuneWrongInitParametersException(NeptuneException):
    pass


class NeptuneRunResumeAndCustomIdCollision(NeptuneWrongInitParametersException):
    def __init__(self):
        message = """
{h1}
----NeptuneRunResumeAndCustomIdCollision-----------------------------------------
{end}
It's not possible to use {python}custom_run_id{end} while resuming a run.

The {python}run{end} and {python}custom_run_id{end} parameters of the {python}init_run(){end} method are mutually exclusive.
Make sure you have no {bash}{custom_id_env}{end} environment variable set
and no value is explicitly passed to the `custom_run_id` argument when you are resuming a run.

You may also want to check the following docs page:
    - https://docs.neptune.ai/logging/to_existing_object/
    - https://docs.neptune.ai/logging/custom_run_id/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(message.format(custom_id_env=CUSTOM_RUN_ID_ENV_NAME, **STYLES))


class NeptuneClientUpgradeRequiredError(NeptuneException):
    def __init__(
        self,
        version: Union[Version, str],
        min_version: Optional[Union[Version, str]] = None,
        max_version: Optional[Union[Version, str]] = None,
    ):
        current_version = str(version)
        required_version = "==" + replace_patch_version(str(max_version)) if max_version else ">=" + str(min_version)
        message = """
{h1}
----NeptuneClientUpgradeRequiredError-------------------------------------------------------------
{end}
Your version of the Neptune client library ({current_version}) is no longer supported by the Neptune
 server. The minimum required version is {required_version}.

In order to update the Neptune client library, run the following command in your terminal:
    {bash}pip install -U neptune-client{end}
Or if you are using Conda, run the following instead:
    {bash}conda update -c conda-forge neptune-client{end}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message.format(
                current_version=current_version,
                required_version=required_version,
                **STYLES,
            )
        )


class NeptuneMissingRequiredInitParameter(NeptuneWrongInitParametersException):
    def __init__(
        self,
        called_function: str,
        parameter_name: str,
    ):
        message = """
{h1}
----NeptuneMissingRequiredInitParameter---------------------------------------
{end}
{python}neptune.{called_function}(){end} invocation was missing {python}{parameter_name}{end}.
If you want to create a new object using {python}{called_function}{end}, {python}{parameter_name}{end} is required:
https://docs.neptune.ai/api/neptune#{called_function}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message.format(
                called_function=called_function,
                parameter_name=parameter_name,
                **STYLES,
            )
        )


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        message = """
{h1}
----CannotResolveHostname-----------------------------------------------------------------------
{end}
The Neptune client library was not able to resolve hostname {underline}{host}{end}.

What should I do?
    - Check if your computer is connected to the internet.
    - Check if your computer is supposed to be using a proxy to access the internet.
      If so, you may want to use the {python}proxies{end} parameter of the {python}init(){end} method.
      See https://docs.neptune.ai/api/universal/#proxies
      and https://requests.readthedocs.io/en/latest/user/advanced/#proxies
    - Check the status of Neptune services: https://status.neptune.ai/

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(host=host, **STYLES))


class NeptuneOfflineModeException(NeptuneException):
    pass


class NeptuneOfflineModeFetchException(NeptuneOfflineModeException):
    def __init__(self):
        message = """
{h1}
----NeptuneOfflineModeFetchException---------------------------------------------------
{end}
It seems you are trying to fetch data from the server while working in offline mode.
You need to work in a non-offline connection mode to fetch data from the server.

You can set the connection mode when creating a new run:
    {python}run = neptune.init_run(mode="async"){end}

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class NeptuneOfflineModeChangeStageException(NeptuneOfflineModeException):
    def __init__(self):
        message = """
{h1}
----NeptuneOfflineModeChangeStageException---------------------------------------
{end}
You cannot change the stage of the model version while in offline mode.
"""
        super().__init__(message.format(**STYLES))


class NeptuneProtectedPathException(NeptuneException):
    extra_info = ""

    def __init__(self, path: str):
        message = """
{h1}
----NeptuneProtectedPathException----------------------------------------------
{end}
Field {path} cannot be changed directly.
{extra_info}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        self._path = path
        super().__init__(
            message.format(
                path=path,
                extra_info=self.extra_info.format(**STYLES),
                **STYLES,
            )
        )


class NeptuneCannotChangeStageManually(NeptuneProtectedPathException):
    extra_info = """
If you want to change the stage of the model version,
use the {python}.change_stage(){end} method:
    {python}model_version.change_stage("staging"){end}"""


class OperationNotSupported(NeptuneException):
    def __init__(self, message: str):
        super().__init__(f"Operation not supported: {message}")


class NeptuneLegacyProjectException(NeptuneException):
    def __init__(self, project: QualifiedName):
        message = """
{h1}
----NeptuneLegacyProjectException---------------------------------------------------------
{end}
Your project "{project}" has not been migrated to the new structure yet.
Unfortunately the neptune.new Python API is incompatible with projects using the old structure,
so please use legacy neptune Python API.
Don't worry - we are working hard on migrating all the projects and you will be able to use the neptune.new API soon.

You can find documentation for the legacy neptune Python API here:
    - https://docs-legacy.neptune.ai/index.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(project=project, **STYLES))


class NeptuneUninitializedException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneUninitializedException----------------------------------------------------
{end}
You must initialize the Neptune client library before you can access `get_last_run`.

Looks like you forgot to add:
    {python}neptune.init_run(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

before you ran:
    {python}neptune.get_last_run(){end}

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/neptune/#get_last_run

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class NeptuneIntegrationNotInstalledException(NeptuneException):
    def __init__(self, integration_package_name, framework_name):
        message = """
{h1}
----NeptuneIntegrationNotInstalledException-----------------------------------------
{end}
Looks like integration {integration_package_name} wasn't installed.
To install, run:
    {bash}pip install {integration_package_name}{end}
Or:
    {bash}pip install "neptune-client[{framework_name}]"{end}

You may also want to check the following docs page:
    - https://docs.neptune.ai/integrations

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(
            message.format(
                integration_package_name=integration_package_name,
                framework_name=framework_name,
                **STYLES,
            )
        )


class NeptuneLimitExceedException(NeptuneException):
    def __init__(self, reason: str):
        message = """
{h1}
----NeptuneLimitExceedException---------------------------------------------------------------------------------------
{end}
{reason}

It's not possible to upload new data, but you can still fetch and delete data.
If you are using asynchronous (default) connection mode, Neptune automatically switched to offline mode
and your data is being stored safely on the disk. You can upload it later using the Neptune Command Line Interface tool:
    {bash}neptune sync -p project_name{end}
What should I do?
    - In case of storage limitations, go to your projects and remove runs or model metadata you don't need
    - ... or update your subscription plan here: https://app.neptune.ai/-/subscription
You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes/
{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES, reason=reason))


class NeptuneFieldCountLimitExceedException(NeptuneException):
    def __init__(self, limit: int, container_type: str, identifier: str):
        message = """
{h1}
----NeptuneFieldCountLimitExceedException---------------------------------------------------------------------------------------
{end}
There are too many fields (more than {limit}) in the {identifier} {container_type}.
We have stopped the synchronization to the Neptune server and stored the data locally.

To continue uploading the metadata:

    1. Delete some excess fields from {identifier}.

       You can delete fields or namespaces with the "del" command.
       For example, to delete the "training/checkpoints" namespace:

       {python}del run["training/checkpoints"]{end}

    2. Once you're done, synchronize the data manually with the following command:

       {bash}neptune sync -p project_name{end}

For more details, see https://docs.neptune.ai/usage/best_practices
"""  # noqa: E501
        super().__init__(
            message.format(
                **STYLES,
                limit=limit,
                container_type=container_type,
                identifier=identifier,
            )
        )


class NeptuneStorageLimitException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneStorageLimitException---------------------------------------------------------------------------------------
{end}
You exceeded the storage limit of the workspace. It's not possible to upload new data, but you can still fetch and delete data.
If you are using asynchronous (default) connection mode, Neptune automatically switched to offline mode
and your data is being stored safely on the disk. You can upload it later using the Neptune Command Line Interface tool:
    {bash}neptune sync -p project_name{end}
What should I do?
    - Go to your projects and remove runs or model metadata you don't need
    - ... or update your subscription plan here: https://app.neptune.ai/-/subscription
You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes
{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(message.format(**STYLES))


class FetchAttributeNotFoundException(MetadataInconsistency):
    def __init__(self, attribute_path: str):
        message = """
{h1}
----MetadataInconsistency----------------------------------------------------------------------
{end}
The field {python}{attribute_path}{end} was not found.

Remember that in the asynchronous (default) connection mode, data is synchronized
with the Neptune servers in the background. The data may have not reached
the servers before it was fetched. Before fetching the data, you can force
wait for all the requests sent by invoking:

    {python}run.wait(){end}

Remember that each use of {python}wait{end} introduces a delay in code execution.

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help.html
"""
        super().__init__(message.format(attribute_path=attribute_path, **STYLES))


class ArtifactNotFoundException(MetadataInconsistency):
    def __init__(self, artifact_hash: str):
        message = """
{h1}
----MetadataInconsistency----------------------------------------------------------------------
{end}
Artifact with hash {python}{artifact_hash}{end} was not found.

Remember that in the asynchronous (default) connection mode, data is synchronized
with the Neptune servers in the background. The data may have not reached
the servers before it was fetched. Before fetching the data, you can force
wait for all the requests sent by invoking:

    {python}run.wait(){end}

Remember that each use of {python}wait{end} introduces a delay in code execution.

You may also want to check the following docs page:
    - https://docs.neptune.ai/api/connection_modes

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help.html
"""
        super().__init__(message.format(artifact_hash=artifact_hash, **STYLES))


class PlotlyIncompatibilityException(Exception):
    def __init__(self, matplotlib_version, plotly_version, details):
        super().__init__(
            "Unable to convert plotly figure to matplotlib format. "
            "Your matplotlib ({}) and plotlib ({}) versions are not compatible. "
            "{}".format(matplotlib_version, plotly_version, details)
        )


class NeptunePossibleLegacyUsageException(NeptuneWrongInitParametersException):
    def __init__(self):
        message = """
{h1}
----NeptunePossibleLegacyUsageException----------------------------------------------------------------
{end}
It seems you are trying to use the legacy API, but you imported the new one.

Simply update your import statement to:
    {python}import neptune{end}

You may want to check the legacy API docs:
    - https://docs-legacy.neptune.ai

If you want to update your code with the new API, we prepared a handy migration guide:
    - https://docs.neptune.ai/about/legacy/#migrating-to-neptunenew

You can read more about neptune.new in the release blog post:
    - https://neptune.ai/blog/neptune-new

You may also want to check the following docs page:
    - https://docs-legacy.neptune.ai/getting-started/integrate-neptune-into-your-codebase.html

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
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
    https://docs.neptune.ai/about/legacy/#migrating-to-neptunenew
    - You can read more about neptune.new in the release blog post:
    https://neptune.ai/blog/neptune-new

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""  # noqa: E501
        super().__init__(message.format(**STYLES))


class NeptuneUnhandledArtifactSchemeException(NeptuneException):
    def __init__(self, path: str):
        scheme = urlparse(path).scheme
        message = """
{h1}
----NeptuneUnhandledArtifactProtocolException------------------------------------
{end}
You have used a Neptune Artifact to track a file with a scheme unhandled by this client ({scheme}).
Problematic path: {path}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(scheme=scheme, path=path, **STYLES))


class NeptuneUnhandledArtifactTypeException(NeptuneException):
    def __init__(self, type_str: str):
        message = """
{h1}
----NeptuneUnhandledArtifactTypeException----------------------------------------
{end}
A Neptune Artifact you're listing is tracking a file type unhandled by this client ({type_str}).

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(type_str=type_str, **STYLES))


class NeptuneLocalStorageAccessException(NeptuneException):
    def __init__(self, path, expected_description):
        message = """
{h1}
----NeptuneLocalStorageAccessException-------------------------------------
{end}
Neptune had a problem processing "{path}". It expects it to be {expected_description}.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(path=path, expected_description=expected_description, **STYLES))


class NeptuneRemoteStorageCredentialsException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneRemoteStorageCredentialsException-------------------------------------
{end}
Neptune could not find suitable credentials for remote storage of a Neptune Artifact you're listing.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class NeptuneRemoteStorageAccessException(NeptuneException):
    def __init__(self, location: str):
        message = """
{h1}
----NeptuneRemoteStorageAccessException------------------------------------------
{end}
Neptune could not access an object ({location}) from remote storage of a Neptune Artifact you're listing.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(location=location, **STYLES))


class ArtifactUploadingError(NeptuneException):
    def __init__(self, msg: str):
        super().__init__("Cannot upload artifact: {}".format(msg))


class NeptuneUnsupportedArtifactFunctionalityException(NeptuneException):
    def __init__(self, functionality_info: str):
        message = """
{h1}
----NeptuneUnsupportedArtifactFunctionality-------------------------------------
{end}
It seems you are using Neptune Artifacts functionality that is currently not supported.

{functionality_info}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(functionality_info=functionality_info, **STYLES))


class NeptuneEmptyLocationException(NeptuneException):
    def __init__(self, location: str, namespace: str):
        message = """
{h1}
----NeptuneEmptyLocationException----------------------------------------------
{end}
Neptune could not find files in the requested location ({location}) during the creation of an Artifact in "{namespace}".

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(location=location, namespace=namespace, **STYLES))


class NeptuneFeatureNotAvailableException(NeptuneException):
    def __init__(self, missing_feature):
        message = """
{h1}
----NeptuneFeatureNotAvailableException----------------------------------------------
{end}
The following feature is not yet supported by the Neptune instance you are using:
{missing_feature}

An update of the Neptune instance is required in order to use it. Please contact your local Neptune administrator
or Neptune support directly (support@neptune.ai) about the upcoming updates.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        self.message = message.format(missing_feature=missing_feature, **STYLES)
        super().__init__(message)


class NeptuneObjectCreationConflict(NeptuneException):
    pass


class NeptuneModelKeyAlreadyExistsError(NeptuneObjectCreationConflict):
    def __init__(self, model_key, models_tab_url):
        message = """
{h1}
----NeptuneModelKeyAlreadyExistsError---------------------------------------------------
{end}
A model with the provided key ({model_key}) already exists in this project. A model key has to be unique
within the project.

You can check all of your models in the project on the Models page:
{models_tab_url}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(model_key=model_key, models_tab_url=models_tab_url, **STYLES))


class NeptuneSynchronizationAlreadyStoppedException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----NeptuneSynchronizationAlreadyStopped---------------------------------------------------
{end}
The synchronization thread had stopped before Neptune could finish uploading the logged metadata.
Your data is stored locally, but you'll need to finish the synchronization manually.
To synchronize with the Neptune servers, enter the following on your command line:

    {bash}neptune sync{end}

For details, see https://docs.neptune.ai/api/neptune_sync/

If the synchronization fails, you may want to check your connection and ensure that you're
within limits by going to your Neptune project settings -> Usage.
If the issue persists, our support is happy to help.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class StreamAlreadyUsedException(NeptuneException):
    def __init__(self):
        message = """
{h1}
----StreamAlreadyUsedException---------------------------------------------------
{end}
A File object created with File.from_stream() has already been logged.
You can only log content from the same stream once.

For more, see https://docs.neptune.ai/api/field_types/#from_stream

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help
"""
        super().__init__(message.format(**STYLES))


class NeptuneUserApiInputException(NeptuneException):
    def __init__(self, message):
        super().__init__(message)
