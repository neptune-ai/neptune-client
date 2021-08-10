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
import hashlib
import unittest
import datetime
import tempfile
from pathlib import Path

from mock import patch, Mock

from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher


class TestFileHasher(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()

        with open(f'{self.temp.name}/test', 'wb') as handler:
            handler.write(b'\xde\xad\xbe\xef')

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_artifact_hash(self):
        artifacts = [
            ArtifactFileData(
                file_path='to/file1',
                file_hash='c38444d2ccff1a7aab3d323fb6234e1b4f0a81ac',
                type="S3",
                metadata={
                    'location': "s3://bucket/path/to/file1",
                    "file_size": 18,
                    "last_modification": datetime.datetime(2021, 8, 9, 10, 22, 53)
                }
            ),
            ArtifactFileData(
                file_path='to/file2',
                file_hash='4347d0f8ba661234a8eadc005e2e1d1b646c9682',
                type="S3",
                metadata={
                    'location': "s3://bucket/path/to/file2",
                    "file_size": 24,
                    "last_modification": datetime.datetime(2021, 8, 9, 10, 32, 12)
                }
            )
        ]

        self.assertEqual("63d995a30adf77a40305ce7c69417866666d7df3", FileHasher.get_artifact_hash(artifacts))
        self.assertEqual("63d995a30adf77a40305ce7c69417866666d7df3", FileHasher.get_artifact_hash(reversed(artifacts)))

    @patch('pathlib.Path.home')
    def test_local_file_hash(self, home):
        home.return_value = Path(self.temp.name)

        self.assertEqual(
            'd78f8bb992a56a597f6c7a1fb918bb78271367eb',
            FileHasher.get_local_file_hash(f'{self.temp.name}/test')
        )

    @patch('pathlib.Path.home')
    def test_local_file_hashed_only_once(self, home):
        home.return_value = Path(self.temp.name)
        hashlib.sha1 = Mock(side_effect=hashlib.sha1)

        hashes = {
            FileHasher.get_local_file_hash(f'{self.temp.name}/test') for _ in range(10)
        }

        self.assertEqual(
            {'d78f8bb992a56a597f6c7a1fb918bb78271367eb'},
            hashes
        )

        self.assertEqual(1, hashlib.sha1.call_count)
