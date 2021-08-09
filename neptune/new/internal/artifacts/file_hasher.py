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
import pathlib
import datetime

from dataclasses import dataclass
from datalite import datalite, fetch_if

from neptune.new.internal.artifacts.types import ArtifactFileData, ArtifactMetadataSerializer
from neptune.new.internal.artifacts.utils import sha1


@datalite(db_path=str(pathlib.Path.home() / ".neptune" / "files.db"))
@dataclass
class FileHash:
    file_path: str
    file_hash: str
    modification_date: str


class FileHasher:
    @classmethod
    def get_local_file_hash(cls, file_path: typing.Union[str, pathlib.Path]) -> str:
        absolute = pathlib.Path(file_path).resolve()
        modification_date = datetime.datetime.fromtimestamp(absolute.stat().st_mtime).strftime('%Y%m%d_%H%M%S')

        found = fetch_if(FileHash, f"file_path = '{str(absolute)}'")
        stored_file_hash = found[0] if found is not None and len(found) > 0 else None

        if stored_file_hash:
            if stored_file_hash.modification_date >= modification_date:
                return stored_file_hash.file_hash
            else:
                computed_hash = sha1(absolute)
                stored_file_hash.file_hash = computed_hash
                stored_file_hash.modification_date = modification_date
                stored_file_hash.update_entry()

                return computed_hash

        computed_hash = sha1(absolute)

        # pylint: disable=no-member
        FileHash(str(absolute), computed_hash, modification_date).create_entry()

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
