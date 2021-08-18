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
import uuid
from typing import List, Tuple, Optional, Type

from neptune.new.internal.operation import Operation, AssignArtifact
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactDriver, ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher
from neptune.new.exceptions import NeptuneException, ArtifactUploadingError


_logger = logging.getLogger(__name__)


def track_artifact_files(
        backend: NeptuneBackend,
        project_uuid: uuid.UUID,
        path: List[str],
        entries: List[Tuple[str, Optional[str]]]
) -> Tuple[Optional[NeptuneException], Optional[Operation]]:
    files: List[ArtifactFileData] = list()

    for entry in entries:
        entry_path, entry_namespace = entry

        driver: Type[ArtifactDriver] = ArtifactDriversMap.match_path(entry_path)
        files.extend(
            driver.get_tracked_files(path=entry_path, namespace=entry_namespace)
        )

    if not files:
        return ArtifactUploadingError("Uploading an empty Artifact"), None

    artifact_hash = FileHasher.get_artifact_hash(files)
    artifact = backend.create_new_artifact(project_uuid, artifact_hash, len(files))

    if not artifact.received_metadata:
        backend.upload_artifact_files_metadata(
            project_uuid,
            artifact_hash,
            files
        )

    return None, AssignArtifact(path=path, hash=artifact_hash)
