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

from neptune.new.internal.artifacts.file_hasher import FileHasher
from neptune.new.internal.artifacts.types import ArtifactDriver, ArtifactFileData, ArtifactFileType
from tests.neptune.new.internal.artifacts.utils import append_non_relative_path


class LocalArtifactDriver(ArtifactDriver):
    @staticmethod
    def get_type() -> str:
        return ArtifactFileType.LOCAL.value

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme in ('file', '')

    @classmethod
    def get_tracked_files(cls, path: str, name: str = None) -> typing.List[ArtifactFileData]:
        parsed_path = urlparse(path).path
        source_location = pathlib.Path(parsed_path)

        stored_files: typing.List[ArtifactFileData] = list()

        try:
            files_to_check = source_location.rglob('*') if source_location.is_dir() else [source_location]
            for file in files_to_check:
                # symlink dirs are omitted by rglob('*')
                if not file.is_file():
                    continue

                stored_files.append(
                    ArtifactFileData(
                        file_path=file.as_posix(),
                        file_hash=FileHasher.get_local_file_hash(file),
                        type=ArtifactFileType.LOCAL.value,
                        metadata={
                            'file_path': f'file://{file.as_posix()}',
                            # TODO: add original location if it's symlink?
                            'last_modified': datetime.fromtimestamp(
                                file.stat().st_mtime
                            ),
                            'file_size': file.stat().st_size,
                        }
                    )
                )
        finally:
            # TODO: handle pathlib exceptions
            pass

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        assert destination.is_dir()

        file_path_str = file_definition.file_path
        file_path = pathlib.Path(file_path_str)
        assert file_path.is_file()
        destination = append_non_relative_path(destination, file_path_str)

        os.makedirs(str(destination.parent), exist_ok=True)
        try:
            destination.symlink_to(file_path)
        finally:
            # TODO
            pass
