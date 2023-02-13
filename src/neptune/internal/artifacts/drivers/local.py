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
__all__ = ["LocalArtifactDriver"]

import os
import pathlib
import typing
from datetime import datetime
from urllib.parse import urlparse

from neptune.exceptions import (
    NeptuneLocalStorageAccessException,
    NeptuneUnsupportedArtifactFunctionalityException,
)
from neptune.internal.artifacts.file_hasher import FileHasher
from neptune.internal.artifacts.types import (
    ArtifactDriver,
    ArtifactFileData,
    ArtifactFileType,
)


class LocalArtifactDriver(ArtifactDriver):
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def get_type() -> str:
        return ArtifactFileType.LOCAL.value

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme in ("file", "")

    @classmethod
    def _serialize_metadata(cls, metadata: typing.Dict[str, typing.Any]) -> typing.Dict[str, str]:
        return {
            "file_path": metadata["file_path"],
            "last_modified": datetime.fromtimestamp(metadata["last_modified"]).strftime(cls.DATETIME_FORMAT),
        }

    @classmethod
    def _deserialize_metadata(cls, metadata: typing.Dict[str, str]) -> typing.Dict[str, typing.Any]:
        return {
            "file_path": metadata["file_path"],
            "last_modified": datetime.strptime(metadata["last_modified"], cls.DATETIME_FORMAT),
        }

    @classmethod
    def get_tracked_files(cls, path: str, destination: str = None) -> typing.List[ArtifactFileData]:
        file_protocol_prefix = "file://"
        if path.startswith(file_protocol_prefix):
            path = path[len(file_protocol_prefix) :]

        if "*" in path:
            raise NeptuneUnsupportedArtifactFunctionalityException(
                f"Wildcard characters (*,?) in location URI ({path}) are not supported."
            )

        source_location = pathlib.Path(path).expanduser()

        stored_files: typing.List[ArtifactFileData] = list()

        files_to_check = source_location.rglob("*") if source_location.is_dir() else [source_location]
        for file in files_to_check:
            # symlink dirs are omitted by rglob('*')
            if not file.is_file():
                continue

            if source_location.is_dir():
                file_path = file.relative_to(source_location).as_posix()
            else:
                file_path = file.name
            file_path = file_path if destination is None else (pathlib.Path(destination) / file_path).as_posix()

            stored_files.append(
                ArtifactFileData(
                    file_path=file_path,
                    file_hash=FileHasher.get_local_file_hash(file),
                    type=ArtifactFileType.LOCAL.value,
                    size=file.stat().st_size,
                    metadata=cls._serialize_metadata(
                        {
                            "file_path": f"file://{file.resolve().as_posix()}",
                            "last_modified": file.stat().st_mtime,
                        }
                    ),
                )
            )

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        parsed_path = urlparse(file_definition.metadata.get("file_path"))
        absolute_path = pathlib.Path(parsed_path.netloc + parsed_path.path)

        if not absolute_path.is_file():
            raise NeptuneLocalStorageAccessException(path=absolute_path, expected_description="an existing file")

        os.makedirs(str(destination.parent), exist_ok=True)
        if destination.exists():
            os.remove(destination)
        destination.symlink_to(absolute_path)
