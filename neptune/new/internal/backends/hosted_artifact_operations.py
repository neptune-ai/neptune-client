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
import logging
import typing
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactDriver, ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher


_logger = logging.getLogger(__name__)


def track_artifact_files(backend: HostedNeptuneBackend, path, name):
    files: typing.Iterable[ArtifactFileData] = ArtifactDriversMap.match_path(path).get_tracked_files(path=path, name=name)
    artifact_hash = FileHasher.get_artifact_hash(files)
    backend.create_new_artifact()
    pass
