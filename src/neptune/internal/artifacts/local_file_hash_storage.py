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
__all__ = ["LocalFileHashStorage"]

import os
import sqlite3 as sql
from dataclasses import dataclass
from pathlib import Path


class LocalFileHashStorage:
    @dataclass
    class LocalFileHash:
        file_path: str
        file_hash: str
        modification_date: str

    def __init__(self):
        db_path = Path.home() / ".neptune" / "files.db"
        os.makedirs(db_path.parent, exist_ok=True)

        self.session = sql.connect(str(db_path))
        self.cursor: sql.Cursor = self.session.cursor()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS local_file_hashes (file_path text, file_hash text, modification_date text)"
        )
        self.session.commit()

    def insert(self, path: Path, computed_hash: str, modification_date: str):
        self.cursor.execute(
            "INSERT INTO local_file_hashes (file_path, file_hash, modification_date) VALUES (?, ?, ?)",
            (str(path), computed_hash, modification_date),
        )
        self.session.commit()

    def fetch_one(self, path: Path) -> "LocalFileHash":
        found = [
            LocalFileHashStorage.LocalFileHash(*row)
            for row in self.cursor.execute(
                "SELECT file_path, file_hash, modification_date FROM local_file_hashes WHERE file_path = ?",
                (str(path),),
            )
        ]

        return found[0] if found is not None and len(found) > 0 else None

    def update(self, path: Path, computed_hash: str, modification_date: str):
        self.cursor.execute(
            "UPDATE local_file_hashes SET file_hash=?, modification_date=? WHERE file_path = ?",
            (computed_hash, modification_date, str(path)),
        )
        self.session.commit()
