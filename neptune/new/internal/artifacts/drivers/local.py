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
import os
import pathlib
import typing
from datetime import datetime
from urllib.parse import urlparse

from neptune.new.exceptions import NeptuneLocalStorageAccessException
from neptune.new.internal.artifacts.file_hasher import FileHasher
from neptune.new.internal.artifacts.types import ArtifactDriver, ArtifactFileData, ArtifactFileType


class LocalArtifactDriver(ArtifactDriver):
    @staticmethod
    def get_type() -> str:
        return ArtifactFileType.LOCAL.value

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme in ('file', '')

    @classmethod
    def _serialize_metadata(cls, metadata: typing.Dict[str, typing.Any]) -> typing.Dict[str, str]:
        return {
            "file_path": metadata['file_path'],
            "last_modified": datetime.fromtimestamp(int(metadata['last_modified'])),
            "file_size": int(metadata['file_size']),
        }

    @classmethod
    def _deserialize_metadata(cls, metadata: typing.Dict[str, str]) -> typing.Dict[str, typing.Any]:
        return {
            "file_path": metadata['file_path'],
            "last_modified": metadata['last_modified'].timestamp(),
            "file_size": str(metadata['file_size']),
        }

    @classmethod
    def get_tracked_files(cls, path: str, namespace: str = None) -> typing.List[ArtifactFileData]:
        parsed_path = urlparse(path).path
        source_location = pathlib.Path(parsed_path)

        stored_files: typing.List[ArtifactFileData] = list()

        files_to_check = source_location.rglob('*') if source_location.is_dir() else [source_location]
        for file in files_to_check:
            # symlink dirs are omitted by rglob('*')
            if not file.is_file():
                continue

            if source_location.is_dir():
                file_path = file.relative_to(source_location).as_posix()
            else:
                file_path = file.name
            file_path = file_path if namespace is None else (pathlib.Path(namespace) / file_path).as_posix()

            stored_files.append(
                ArtifactFileData(
                    file_path=file_path,
                    file_hash=FileHasher.get_local_file_hash(file),
                    type=ArtifactFileType.LOCAL.value,
                    metadata=cls._serialize_metadata({
                        'file_path': f'file://{file.resolve().as_posix()}',
                        'last_modified': file.stat().st_mtime,
                        'file_size': file.stat().st_size,
                    })
                )
            )

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        file_path_str = file_definition.file_path
        file_path = pathlib.Path(file_path_str)
        if not file_path.is_file():
            raise NeptuneLocalStorageAccessException(
                path=file_path_str,
                expected_description="an existing file"
            )

        os.makedirs(str(destination.parent), exist_ok=True)
        destination.symlink_to(file_path)
