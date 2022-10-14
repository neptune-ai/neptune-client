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
import dataclasses
import io
import math
import os
import tarfile
from typing import (
    Any,
    Generator,
    Optional,
)

from neptune.common.backends.api_model import MultipartConfig
from neptune.common.exceptions import (
    InternalClientError,
    UploadedFileChanged,
)


@dataclasses.dataclass
class FileChunk:
    data: bytes
    start: int
    end: int


class FileChunker:
    def __init__(self, filename: Optional[str], fobj, total_size, multipart_config: MultipartConfig):
        self._filename: Optional[str] = filename
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
                f"File {self._filename or 'stream'} is too big to upload:"
                f" {self._total_size} bytes exceeds max size {max_size}"
            )
        if self._total_size <= self._max_chunk_count * self._min_chunk_size:
            # can be done as minimal size chunks -- go for it!
            return self._min_chunk_size
        else:
            # need larger chunks -- split more or less equally
            return math.ceil(self._total_size / self._max_chunk_count)

    def generate(self) -> Generator[FileChunk, Any, None]:
        chunk_size = self._get_chunk_size()
        last_offset = 0
        last_change: Optional = os.stat(self._filename).st_mtime if self._filename else None
        while last_offset < self._total_size:
            chunk = self._fobj.read(chunk_size)
            if chunk:
                if last_change and last_change < os.stat(self._filename).st_mtime:
                    raise UploadedFileChanged(self._filename)
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                new_offset = last_offset + len(chunk)
                yield FileChunk(data=chunk, start=last_offset, end=new_offset)
                last_offset = new_offset


def compress_to_tar_gz_in_memory(upload_entries) -> bytes:
    f = io.BytesIO(b"")

    with tarfile.TarFile.open(fileobj=f, mode="w|gz", dereference=True) as archive:
        for entry in upload_entries:
            archive.add(name=entry.source, arcname=entry.target_path, recursive=True)

    f.seek(0)
    data = f.read()
    return data
