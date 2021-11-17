#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

import io
import tarfile
from typing import AnyStr, Union, BinaryIO, Any, Generator

from future.builtins import object

from neptune.internal.storage.storage_utils import (
    UploadEntry,
    AttributeUploadConfiguration,
)


class FileChunk(object):
    def __init__(self, data: AnyStr, start, end):
        self.data = data
        self.start = start
        self.end = end

    def get_data(self) -> AnyStr:
        return self.data

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class FileChunkStream(object):
    def __init__(
        self,
        upload_entry: UploadEntry,
        upload_configuration: AttributeUploadConfiguration,
    ):
        self.filename: str = upload_entry.target_path
        self.upload_configuration: AttributeUploadConfiguration = upload_configuration
        self.length: int = upload_entry.length()
        self.fobj: Union[BinaryIO, io.BytesIO] = upload_entry.get_stream()
        self.permissions: str = upload_entry.get_permissions()

    def __eq__(self, fs):
        if isinstance(self, fs.__class__):
            return self.__dict__ == fs.__dict__
        return False

    def generate(self) -> Generator[FileChunk, Any, None]:
        last_offset = 0
        while True:
            chunk = self.fobj.read(self.upload_configuration.chunk_size)
            if chunk:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                new_offset = last_offset + len(chunk)
                yield FileChunk(chunk, last_offset, new_offset)
                last_offset = new_offset
            else:
                if last_offset == 0:
                    yield FileChunk(b"", 0, 0)
                break

    def close(self):
        self.fobj.close()


def compress_to_tar_gz_in_memory(upload_entries) -> bytes:
    f = io.BytesIO(b"")

    with tarfile.TarFile.open(fileobj=f, mode="w|gz", dereference=True) as archive:
        for entry in upload_entries:
            archive.add(
                name=entry.source_path, arcname=entry.target_path, recursive=True
            )

    f.seek(0)
    data = f.read()
    return data
