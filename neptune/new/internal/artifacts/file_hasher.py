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
import typing
import hashlib
import datetime
from pathlib import Path

from neptune.new.internal.artifacts.types import ArtifactFileData, ArtifactMetadataSerializer
from neptune.new.internal.artifacts.local_file_hash_storage import LocalFileHashStorage
from neptune.new.internal.artifacts.utils import sha1


class FileHasher:
    local_storage = LocalFileHashStorage()

    @classmethod
    def get_local_file_hash(cls, file_path: typing.Union[str, Path]) -> str:
        absolute = Path(file_path).resolve()
        modification_date = datetime.datetime.fromtimestamp(absolute.stat().st_mtime).strftime('%Y%m%d_%H%M%S%f')

        stored_file_hash = FileHasher.local_storage.fetch_one(absolute)

        if stored_file_hash:
            if stored_file_hash.modification_date >= modification_date:
                return stored_file_hash.file_hash
            else:
                computed_hash = sha1(absolute)
                FileHasher.local_storage.update(absolute, computed_hash, modification_date)

                return computed_hash
        else:
            computed_hash = sha1(absolute)
            FileHasher.local_storage.insert(absolute, computed_hash, modification_date)

            return computed_hash

    @classmethod
    def get_artifact_hash(cls, artifact_files: typing.Iterable[ArtifactFileData]) -> str:
        artifact_hash = hashlib.sha1()

        for artifact_file in sorted(artifact_files, key=lambda file: file.file_path):
            artifact_hash.update(artifact_file.file_path.encode())
            artifact_hash.update(artifact_file.file_hash.encode())
            artifact_hash.update(artifact_file.type.encode())

            for metadata_name, metadata_value in ArtifactMetadataSerializer.serialize(artifact_file.metadata):
                artifact_hash.update(metadata_name.encode())
                artifact_hash.update(metadata_value.encode())

        return str(artifact_hash.hexdigest())
