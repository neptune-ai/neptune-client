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
import uuid
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactDriver, ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher


_logger = logging.getLogger(__name__)


def track_artifact_files(backend: NeptuneBackend, project_uuid: uuid.UUID, path, namespace):
    files: typing.List[ArtifactFileData] = ArtifactDriversMap.match_path(path).get_tracked_files(path=path, namespace=namespace)
    import os
    print(os.getcwd())
    print(files)
    artifact_hash = FileHasher.get_artifact_hash(files)
    print(artifact_hash)
    artifact = backend.create_new_artifact(project_uuid, artifact_hash, len(files))
    if not artifact.received_metadata:
        backend.upload_artifact_files_metadata(
            project_uuid,
            artifact_hash,
            files
        )

    # artifact_hash = '81ce0fb16f8b233dd147092d3884623c6b182696'
    # files = backend.list_artifact_files(project_uuid=project_uuid, artifact_hash=artifact_hash)
    # print(files)
