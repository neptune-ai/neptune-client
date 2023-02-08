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

# backwards compatibility

__all__ = [
    "ApiExperiment",
    "CannotSynchronizeOfflineRunsWithoutProject",
    "DiskQueue",
    "HostedNeptuneBackend",
    "NeptuneBackend",
    "NeptuneConnectionLostException",
    "NeptuneException",
    "Operation",
    "Path",
    "Project",
    "ProjectNotFound",
    "RunNotFound",
    "status",
    "sync",
    "AbstractBackendRunner",
    "StatusRunner",
    "SyncRunner",
    "get_metadata_container",
    "get_offline_dirs",
    "get_project",
    "get_qualified_name",
    "is_container_synced_and_remove_junk",
    "iterate_containers",
    "split_dir_name",
]

from neptune.common.deprecation import warn_once
from neptune.common.exceptions import NeptuneConnectionLostException
from neptune.new.cli.commands import (
    ApiExperiment,
    CannotSynchronizeOfflineRunsWithoutProject,
    DiskQueue,
    HostedNeptuneBackend,
    NeptuneBackend,
    NeptuneException,
    Operation,
    Path,
    Project,
    ProjectNotFound,
    RunNotFound,
    status,
    sync,
)
from neptune.new.sync.abstract_backend_runner import AbstractBackendRunner
from neptune.new.sync.status import StatusRunner
from neptune.new.sync.sync import SyncRunner
from neptune.new.sync.utils import (
    get_metadata_container,
    get_offline_dirs,
    get_project,
    get_qualified_name,
    is_container_synced_and_remove_junk,
    iterate_containers,
    split_dir_name,
)

warn_once(
    message="You're using a legacy neptune.new.sync package."
    " It will be removed since `neptune-client==1.0.0`."
    " Please use neptune.new.cli"
)
