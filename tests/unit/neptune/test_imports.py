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
# fmt: off
# flake8: noqa
import unittest

from neptune.attributes.atoms.artifact import Artifact
from neptune.attributes.atoms.atom import Atom
from neptune.attributes.atoms.boolean import Boolean
from neptune.attributes.atoms.datetime import Datetime
from neptune.attributes.atoms.file import File
from neptune.attributes.atoms.float import Float
from neptune.attributes.atoms.git_ref import GitRef
from neptune.attributes.atoms.integer import Integer
from neptune.attributes.atoms.notebook_ref import NotebookRef
from neptune.attributes.atoms.run_state import RunState
from neptune.attributes.atoms.string import String
from neptune.attributes.attribute import Attribute
from neptune.attributes.file_set import FileSet
from neptune.attributes.namespace import (
    Namespace,
    NamespaceBuilder,
)
from neptune.attributes.series.fetchable_series import FetchableSeries
from neptune.attributes.series.file_series import FileSeries
from neptune.attributes.series.float_series import FloatSeries
from neptune.attributes.series.series import Series
from neptune.attributes.series.string_series import StringSeries
from neptune.attributes.sets.set import Set
from neptune.attributes.sets.string_set import StringSet
from neptune.attributes.utils import create_attribute_from_type
from neptune.exceptions import (
    AmbiguousProjectName,
    ArtifactNotFoundException,
    ArtifactUploadingError,
    CannotResolveHostname,
    CannotSynchronizeOfflineRunsWithoutProject,
    ClientHttpError,
    ExceptionWithProjectsWorkspacesListing,
    FetchAttributeNotFoundException,
    FileNotFound,
    FileSetUploadError,
    FileUploadError,
    Forbidden,
    InactiveRunException,
    InternalClientError,
    InternalServerError,
    MalformedOperation,
    MetadataInconsistency,
    MissingFieldException,
    NeedExistingRunForReadOnlyMode,
    NeptuneApiException,
    NeptuneClientUpgradeRequiredError,
    NeptuneConnectionLostException,
    NeptuneEmptyLocationException,
    NeptuneException,
    NeptuneFeatureNotAvailableException,
    NeptuneIntegrationNotInstalledException,
    NeptuneInvalidApiTokenException,
    NeptuneLegacyIncompatibilityException,
    NeptuneLegacyProjectException,
    NeptuneLimitExceedException,
    NeptuneLocalStorageAccessException,
    NeptuneMissingApiTokenException,
    NeptuneMissingProjectNameException,
    NeptuneOfflineModeFetchException,
    NeptunePossibleLegacyUsageException,
    NeptuneRemoteStorageAccessException,
    NeptuneRemoteStorageCredentialsException,
    NeptuneRunResumeAndCustomIdCollision,
    NeptuneSSLVerificationError,
    NeptuneStorageLimitException,
    NeptuneUnhandledArtifactSchemeException,
    NeptuneUnhandledArtifactTypeException,
    NeptuneUninitializedException,
    NeptuneUnsupportedArtifactFunctionalityException,
    OperationNotSupported,
    PlotlyIncompatibilityException,
    ProjectNotFound,
    RunNotFound,
    RunUUIDNotFound,
    Unauthorized,
)
from neptune.handler import Handler
from neptune.integrations.python_logger import NeptuneHandler
from neptune.logging.logger import Logger
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
from neptune.project import Project
from neptune.run import (
    Attribute,
    Boolean,
    Datetime,
    Float,
    Handler,
    InactiveRunException,
    Integer,
    MetadataInconsistency,
    Namespace,
    NamespaceAttr,
    NamespaceBuilder,
    NeptunePossibleLegacyUsageException,
    Run,
    RunState,
    String,
    Value,
)
from neptune.runs_table import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
    LeaderboardHandler,
    MetadataInconsistency,
    RunsTable,
    RunsTableEntry,
)
from neptune.sync import (
    ApiExperiment,
    CannotSynchronizeOfflineRunsWithoutProject,
    DiskQueue,
    HostedNeptuneBackend,
    NeptuneBackend,
    NeptuneConnectionLostException,
    NeptuneException,
    Operation,
    Path,
    Project,
    ProjectNotFound,
    RunNotFound,
)
from neptune.types.atoms.artifact import Artifact
from neptune.types.atoms.atom import Atom
from neptune.types.atoms.boolean import Boolean
from neptune.types.atoms.datetime import Datetime
from neptune.types.atoms.file import File
from neptune.types.atoms.float import Float
from neptune.types.atoms.git_ref import GitRef
from neptune.types.atoms.integer import Integer
from neptune.types.atoms.string import String
from neptune.types.file_set import FileSet
from neptune.types.namespace import Namespace
from neptune.types.series.file_series import FileSeries
from neptune.types.series.float_series import FloatSeries
from neptune.types.series.series import Series
from neptune.types.series.series_value import SeriesValue
from neptune.types.series.string_series import StringSeries
from neptune.types.sets.set import Set
from neptune.types.sets.string_set import StringSet
from neptune.types.value import Value
from neptune.types.value_visitor import ValueVisitor


class TestImports(unittest.TestCase):
    def test_imports(self):
        pass
# fmt: on
