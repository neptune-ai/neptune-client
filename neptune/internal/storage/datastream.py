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
import dataclasses
import io
import tarfile
from typing import Union, BinaryIO, Any, Generator

from future.builtins import object

from neptune.internal.storage.storage_utils import (
    UploadEntry,
    AttributeUploadConfiguration,
)
from neptune.new.exceptions import InternalClientError
from neptune.new.internal.backends.api_model import MultipartConfig


@dataclasses.dataclass
class FileChunk:
    data: bytes
    start: int
    end: int


class FileChunker:
    def __init__(self, filename, fobj, total_size, multipart_config: MultipartConfig):
        self._filename = filename
        self._fobj = fobj
        self._total_size = total_size
        self._min_chunk_size = multipart_config.min_chunk_size
        self._max_chunk_size = multipart_config.max_chunk_size
        self._max_chunk_count = multipart_config.max_chunk_count

    def _get_chunk_size(self) -> int:
        if self._total_size > self._max_chunk_count * self._max_chunk_size:
            # can't fit it
            max_size = self._max_chunk_count * self._max_chunk_size
            raise InternalClientError(
                f"File {self._filename} is too big to upload:"
                f" {self._total_size} bytes exceeds max size {max_size}"
            )
        if self._total_size < self._max_chunk_count * self._min_chunk_size:
            # can be done as minimal size chunks -- go for it!
            return self._min_chunk_size
        else:
            # need larger chunks -- split more or less equally
            return self._total_size // (self._max_chunk_count + 1)

    def generate(self) -> Generator[FileChunk, Any, None]:
        chunk_size = self._get_chunk_size()
        last_offset = 0
        while last_offset < self._total_size:
            chunk = self._fobj.read(chunk_size)
            if chunk:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                new_offset = last_offset + len(chunk)
                yield FileChunk(data=chunk, start=last_offset, end=new_offset)
                last_offset = new_offset


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
                yield FileChunk(data=chunk, start=last_offset, end=new_offset)
                last_offset = new_offset
            else:
                if last_offset == 0:
                    yield FileChunk(data=b"", start=0, end=0)
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
