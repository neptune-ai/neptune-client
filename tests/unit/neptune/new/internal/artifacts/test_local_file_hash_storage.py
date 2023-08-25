#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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

import datetime
import tempfile
import unittest
from pathlib import Path

from mock import patch

from neptune.internal.artifacts.local_file_hash_storage import LocalFileHashStorage


class TestLocalFileHashStorage(unittest.TestCase):
    @patch("pathlib.Path.home")
    def setUp(self, home) -> None:
        self.tempDir = tempfile.TemporaryDirectory()
        home.return_value = Path(self.tempDir.name)

        self.sut = LocalFileHashStorage()
        self.sut.insert(
            Path(f"{self.tempDir.name}/test.file"),
            "c38444d2ccff1a7aab3d323fb6234e1b4f0a81ac",
            datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f"),
        )

    def test_fetch_presented(self):
        returned = self.sut.fetch_one(Path(f"{self.tempDir.name}/test.file"))

        self.assertEqual(returned.file_hash, "c38444d2ccff1a7aab3d323fb6234e1b4f0a81ac")

    def test_fetch_not_presented(self):
        returned = self.sut.fetch_one(Path(f"{self.tempDir.name}/test1.file"))

        self.assertIsNone(returned)

    def test_update_for_not_presented_suppressed(self):
        self.sut.update(
            Path(f"{self.tempDir.name}/test1.file"),
            "c38444d2ccff1a7aab3d323fb6234e1b4f0a81ac",
            datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f"),
        )

    def test_update_for_presented(self):
        new_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")

        self.sut.update(Path(f"{self.tempDir.name}/test.file"), "new_test_hash", new_datetime)
        returned = self.sut.fetch_one(Path(f"{self.tempDir.name}/test.file"))

        self.assertEqual(returned.file_hash, "new_test_hash")
        self.assertEqual(returned.modification_date, new_datetime)

    def test_insert(self):
        modification_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")

        self.sut.insert(Path(f"{self.tempDir.name}/test23.file"), "test_hash", modification_date)
        returned = self.sut.fetch_one(Path(f"{self.tempDir.name}/test23.file"))

        self.assertEqual(returned.file_hash, "test_hash")
        self.assertEqual(returned.modification_date, modification_date)
