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
# isort:skip
# pylint: disable=unused-import,reimported,import-error,no-name-in-module
# fmt: off
# flake8: noqa
import unittest

from neptune.api_exceptions import (
    ChannelAlreadyExists,
    ChannelDoesNotExist,
    ChannelNotFound,
    ChannelsValuesSendBatchError,
    ConnectionLost,
    ExperimentAlreadyFinished,
    ExperimentLimitReached,
    ExperimentNotFound,
    ExperimentOperationErrors,
    ExperimentValidationError,
    Forbidden,
    InvalidApiKey,
    NeptuneApiException,
    NeptuneSSLVerificationError,
    NotebookNotFound,
    PathInExperimentNotFound,
    PathInProjectNotFound,
    ProjectNotFound,
    ServerError,
    StorageLimitReached,
    Unauthorized,
    WorkspaceNotFound,
)
from neptune.backend import (
    ApiClient,
    BackendApiClient,
    LeaderboardApiClient,
)
from neptune.checkpoint import Checkpoint
from neptune.exceptions import (
    CannotResolveHostname,
    DeleteArtifactUnsupportedInAlphaException,
    DeprecatedApiToken,
    DownloadArtifactsUnsupportedException,
    DownloadArtifactUnsupportedException,
    DownloadSourcesException,
    FileNotFound,
    InvalidChannelValue,
    InvalidNeptuneBackend,
    InvalidNotebookPath,
    NeptuneException,
    NeptuneIncorrectImportException,
    NeptuneIncorrectProjectQualifiedNameException,
    NeptuneLibraryNotInstalledException,
    NeptuneMissingApiTokenException,
    NeptuneMissingProjectQualifiedNameException,
    NeptuneNoExperimentContextException,
    NeptuneUninitializedException,
    NoChannelValue,
    NotADirectory,
    NotAFile,
    UnsupportedClientVersion,
    UnsupportedInAlphaException,
)
from neptune.experiments import Experiment
from neptune.git_info import GitInfo
from neptune.management.exceptions import (
    AccessRevokedOnDeletion,
    AccessRevokedOnMemberRemoval,
    BadRequestException,
    ConflictingWorkspaceName,
    InvalidProjectName,
    ManagementOperationFailure,
    MissingWorkspaceName,
    ProjectAlreadyExists,
    ProjectNotFound,
    ProjectsLimitReached,
    UnsupportedValue,
    UserAlreadyHasAccess,
    UserNotExistsOrWithoutAccess,
    WorkspaceNotFound,
)
from neptune.model import (
    ChannelWithLastValue,
    LeaderboardEntry,
    Point,
    Points,
)
from neptune.notebook import Notebook
from neptune.oauth import (
    NeptuneAuth,
    NeptuneAuthenticator,
)
from neptune.projects import Project
from neptune.sessions import Session
from neptune.utils import NoopObject


class TestImports(unittest.TestCase):
    def test_imports(self):
        pass
# fmt: on
