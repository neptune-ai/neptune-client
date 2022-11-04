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
from neptune.new.attributes.atoms.artifact import (
    Artifact,
    ArtifactDriver,
    ArtifactDriversMap,
    ArtifactFileData,
    ArtifactVal,
    AssignArtifact,
    Atom,
    OptionalFeatures,
    TrackFilesToArtifact,
)
from neptune.new.attributes.atoms.atom import (
    Atom,
    Attribute,
)
from neptune.new.attributes.atoms.boolean import (
    AssignBool,
    Boolean,
    BooleanVal,
)
from neptune.new.attributes.atoms.datetime import (
    AssignDatetime,
    Datetime,
    DatetimeVal,
    datetime,
)
from neptune.new.attributes.atoms.file import (
    Atom,
    File,
    FileVal,
    UploadFile,
)
from neptune.new.attributes.atoms.float import (
    AssignFloat,
    Float,
    FloatVal,
)
from neptune.new.attributes.atoms.git_ref import (
    Atom,
    GitRef,
)
from neptune.new.attributes.atoms.integer import (
    AssignInt,
    Integer,
    IntegerVal,
)
from neptune.new.attributes.atoms.notebook_ref import (
    Atom,
    NotebookRef,
)
from neptune.new.attributes.atoms.run_state import (
    Atom,
    RunState,
)
from neptune.new.attributes.atoms.string import (
    AssignString,
    String,
    StringVal,
)
from neptune.new.attributes.attribute import (
    Attribute,
    List,
    NeptuneBackend,
    Operation,
)
from neptune.new.attributes.file_set import (
    Attribute,
    DeleteFiles,
    FileSet,
    FileSetVal,
    Iterable,
    UploadFileSet,
)
from neptune.new.attributes.namespace import (
    Attribute,
    Dict,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Namespace,
    NamespaceBuilder,
    NamespaceVal,
    NoValue,
    RunStructure,
)
from neptune.new.attributes.series.fetchable_series import (
    Dict,
    FetchableSeries,
    FloatSeriesValues,
    Generic,
    StringSeriesValues,
    TypeVar,
    datetime,
)
from neptune.new.attributes.series.file_series import (
    ClearImageLog,
    Data,
    File,
    FileNotFound,
    FileSeries,
    FileSeriesVal,
    ImageValue,
    Iterable,
    LogImages,
    Operation,
    OperationNotSupported,
    Series,
    Val,
)
from neptune.new.attributes.series.float_series import (
    ClearFloatLog,
    ConfigFloatSeries,
    FetchableSeries,
    FloatSeries,
    FloatSeriesVal,
    FloatSeriesValues,
    Iterable,
    LogFloats,
    Operation,
    Series,
    Val,
)
from neptune.new.attributes.series.series import (
    Attribute,
    Generic,
    Iterable,
    Operation,
    Series,
    SeriesVal,
    TypeVar,
)
from neptune.new.attributes.series.string_series import (
    ClearStringLog,
    Data,
    FetchableSeries,
    Iterable,
    List,
    LogStrings,
    Operation,
    Series,
    StringSeries,
    StringSeriesVal,
    StringSeriesValues,
    Val,
)
from neptune.new.attributes.sets.set import (
    Attribute,
    Set,
)
from neptune.new.attributes.sets.string_set import (
    AddStrings,
    ClearStringSet,
    Iterable,
    RemoveStrings,
    Set,
    StringSet,
    StringSetVal,
)
from neptune.new.attributes.utils import (
    Artifact,
    AttributeType,
    Boolean,
    Datetime,
    File,
    FileSeries,
    FileSet,
    Float,
    FloatSeries,
    GitRef,
    Integer,
    InternalClientError,
    List,
    NotebookRef,
    RunState,
    String,
    StringSeries,
    StringSet,
)
from neptune.new.cli import (
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
from neptune.new.exceptions import (
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
    List,
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
    Project,
    ProjectNotFound,
    RunNotFound,
    RunUUIDNotFound,
    Unauthorized,
    Version,
    Workspace,
)
from neptune.new.handler import (
    Artifact,
    ArtifactFileData,
    File,
    FileSeries,
    FileSet,
    FileVal,
    FloatSeries,
    Handler,
    NeptuneException,
    StringSeries,
    StringSet,
)
from neptune.new.integrations.python_logger import (
    Logger,
    NeptuneHandler,
    Run,
    RunState,
)
from neptune.new.logging.logger import Logger
from neptune.new.project import Project
from neptune.new.run import (
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
from neptune.new.runs_table import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
    LeaderboardHandler,
    MetadataInconsistency,
    RunsTable,
    RunsTableEntry,
)
from neptune.new.types.atoms.artifact import (
    Artifact,
    Atom,
    FileHasher,
    TypeVar,
)
from neptune.new.types.atoms.atom import (
    Atom,
    TypeVar,
    Value,
)
from neptune.new.types.atoms.boolean import (
    Atom,
    Boolean,
    TypeVar,
)
from neptune.new.types.atoms.datetime import (
    Atom,
    Datetime,
    TypeVar,
    datetime,
)
from neptune.new.types.atoms.file import (
    Atom,
    File,
    IOBase,
    TypeVar,
)
from neptune.new.types.atoms.float import (
    Atom,
    Float,
    TypeVar,
)
from neptune.new.types.atoms.git_ref import (
    Atom,
    GitRef,
    List,
    TypeVar,
    datetime,
)
from neptune.new.types.atoms.integer import (
    Atom,
    Integer,
    TypeVar,
)
from neptune.new.types.atoms.string import (
    Atom,
    String,
    TypeVar,
)
from neptune.new.types.file_set import (
    FileSet,
    Iterable,
    List,
    TypeVar,
    Value,
)
from neptune.new.types.namespace import (
    Namespace,
    TypeVar,
    Value,
)
from neptune.new.types.series.file_series import (
    File,
    FileSeries,
    List,
    Series,
    TypeVar,
)
from neptune.new.types.series.float_series import (
    FloatSeries,
    Series,
    TypeVar,
)
from neptune.new.types.series.series import (
    Series,
    TypeVar,
    Value,
)
from neptune.new.types.series.series_value import (
    Generic,
    SeriesValue,
    TypeVar,
)
from neptune.new.types.series.string_series import (
    Series,
    StringSeries,
    TypeVar,
)
from neptune.new.types.sets.set import (
    Set,
    TypeVar,
    Value,
)
from neptune.new.types.sets.string_set import (
    Iterable,
    Set,
    StringSet,
    TypeVar,
)
from neptune.new.types.value import (
    TypeVar,
    Value,
)
from neptune.new.types.value_visitor import (
    Artifact,
    Boolean,
    Datetime,
    File,
    FileSeries,
    FileSet,
    Float,
    FloatSeries,
    Generic,
    GitRef,
    Integer,
    Namespace,
    String,
    StringSeries,
    StringSet,
    TypeVar,
    Value,
    ValueVisitor,
)
from neptune.notebook import Notebook


class TestImports(unittest.TestCase):
    def test_imports(self):
        pass
# fmt: on
