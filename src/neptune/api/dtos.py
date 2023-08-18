__all__ = ["FileEntry"]

import datetime
from dataclasses import dataclass
from typing import Any


@dataclass
class FileEntry:
    name: str
    size: int
    mtime: datetime.datetime
    file_type: str

    @classmethod
    def from_dto(cls, file_dto: Any) -> "FileEntry":
        return cls(name=file_dto.name, size=file_dto.size, mtime=file_dto.mtime, file_type=file_dto.fileType)
