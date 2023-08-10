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
__all__ = [
    "FileComposite",
    "LocalFileComposite",
    "InMemoryComposite",
    "FileComposite",
    "StreamComposite",
]

import abc
import enum
import io
import os
from functools import wraps
from io import IOBase
from typing import (
    Any,
    Callable,
    Optional,
    Union,
)

from neptune.common.exceptions import NeptuneException
from neptune.exceptions import StreamAlreadyUsedException
from neptune.internal.utils import verify_type


class FileType(enum.Enum):
    LOCAL_FILE = "LOCAL_FILE"
    IN_MEMORY = "IN_MEMORY"
    STREAM = "STREAM"


class FileComposite(abc.ABC):
    """
    Composite class defining behaviour of neptune.types.atoms.file.File
    """

    file_type: FileType

    def __init__(self, extension: str) -> None:
        verify_type("extension", extension, str)

        self._extension: str = extension

    @property
    def extension(self) -> str:
        return self._extension

    @property
    def path(self) -> str:
        raise NeptuneException(f"`path` attribute is not supported for {self.file_type}")

    @property
    def content(self) -> Union[str, bytes]:
        raise NeptuneException(f"`content` attribute is not supported for {self.file_type}")

    def save(self, path: str) -> None:
        raise NeptuneException(f"`save` method is not supported for {self.file_type}")


class LocalFileComposite(FileComposite):
    file_type = FileType.LOCAL_FILE

    def __init__(self, path: str, extension: Optional[str] = None) -> None:
        if extension:
            normalized_extension = extension
        else:
            try:
                normalized_extension = os.path.splitext(path)[1]
                normalized_extension = normalized_extension[1:] if normalized_extension else ""
            except ValueError:
                normalized_extension = ""

        super().__init__(extension=normalized_extension)

        self._path: str = path

    @property
    def path(self) -> str:
        return self._path

    def __str__(self) -> str:
        return f"File(path={self.path})"


class InMemoryComposite(FileComposite):
    file_type = FileType.IN_MEMORY

    def __init__(self, content: Union[str, bytes], extension: Optional[str] = None) -> None:
        if isinstance(content, str):
            normalized_extension: str = "txt"
            content = content.encode("utf-8")
        else:
            normalized_extension = "bin"

        super().__init__(extension=extension or normalized_extension)

        self._content: bytes = content

    @property
    def content(self) -> bytes:
        return self._content

    def save(self, path: str) -> None:
        with open(path, "wb") as handler:
            handler.write(self._content)

    def __str__(self) -> str:
        return "File(content=...)"


def read_once(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for validating read once on STREAM objects"""

    @wraps(f)
    def func(self: "StreamComposite", *args: Any, **kwargs: Any) -> Any:
        if self._stream_read:
            raise StreamAlreadyUsedException()
        self._stream_read = True
        return f(self, *args, **kwargs)

    return func


class StreamComposite(FileComposite):
    file_type = FileType.STREAM

    def __init__(self, stream: IOBase, seek: Optional[int] = 0, extension: Optional[str] = None) -> None:
        verify_type("stream", stream, IOBase)
        verify_type("extension", extension, (str, type(None)))

        if seek is not None and stream.seekable():
            stream.seek(seek)

        normalized_extension = extension or "txt" if isinstance(stream, io.TextIOBase) else "bin"

        super().__init__(extension=normalized_extension)

        self._stream: IOBase = stream
        self._stream_read: bool = False

    @property
    @read_once
    def content(self) -> bytes:
        if isinstance(self._stream, io.TextIOBase):
            return self._stream.read().encode("utf-8")

        if isinstance(self._stream, io.BufferedIOBase):
            return self._stream.read()

        if isinstance(self._stream, io.RawIOBase):
            return self._stream.read() or b""

        raise NeptuneException(f"Unsupported stream type: {type(self._stream)}")

    @read_once
    def save(self, path: str) -> None:
        with open(path, "wb") as handler:
            stream_buffer = self._stream.read(io.DEFAULT_BUFFER_SIZE)
            while stream_buffer:
                # TODO: replace with Walrus Operator once python3.7 support is dropped
                if isinstance(self._stream, io.TextIOBase):
                    stream_buffer = stream_buffer.encode("utf-8")

                handler.write(stream_buffer)
                stream_buffer = self._stream.read(io.DEFAULT_BUFFER_SIZE)

    def __str__(self) -> str:
        return f"File(stream={self._stream})"
