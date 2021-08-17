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


def track_artifact_files(backend: NeptuneBackend, run_uuid: uuid.UUID, path, namespace):
    api_run = backend.get_run(run_id=str(run_uuid))
    project_qualified_name = f'{api_run.workspace}/{api_run.project_name}'

    driver: ArtifactDriver = ArtifactDriversMap.match_path(path)
    files: typing.List[ArtifactFileData] = driver.get_tracked_files(path=path, namespace=namespace)
    artifact_hash = FileHasher.get_artifact_hash(files)

    artifact = backend.create_new_artifact(project_qualified_name, artifact_hash, len(files))

    if not artifact.received_metadata:
        backend.upload_artifact_files_metadata(
            project_qualified_name,
            artifact_hash,
            files
        )
