import datetime
from dataclasses import dataclass

from neptune.api.dtos import FileEntry


def test_file_entry_from_dto():
    now = datetime.datetime.now()

    @dataclass
    class MockDto:
        name: str
        size: int
        mtime: datetime.datetime
        fileType: str

    dto = MockDto("mock_name", 100, now, "file")

    entry = FileEntry.from_dto(dto)

    assert entry.name == "mock_name"
    assert entry.size == 100
    assert entry.mtime == now
    assert entry.file_type == "file"
