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
import time
import hashlib
import unittest
import tempfile
from pathlib import Path

from mock import patch, Mock

from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher


class TestFileHasher(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()

        with open(f"{self.temp.name}/test", "wb") as handler:
            handler.write(b"\xde\xad\xbe\xef")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_artifact_hash(self):
        # do not change this test case without coordinating with Artifact API's ArtifactHashComputer
        artifacts = [
            ArtifactFileData(
                file_path="to/file1",
                file_hash="c38444d2ccff1a7aab3d323fb6234e1b4f0a81ac",
                type="S3",
                size=5234,
                metadata={
                    "location": "s3://bucket/path/to/file1",
                    "last_modification": "2021-08-09 10:22:53",
                },
            ),
            ArtifactFileData(
                file_path="from/file2",
                file_hash="4347d0f8ba661234a8eadc005e2e1d1b646c9682",
                type="S3",
                metadata={
                    "location": "s3://bucket/path/to/file2",
                    "last_modification": "2021-08-09 10:32:12",
                },
            ),
        ]

        expected_hash = (
            "56e64245b1d4915ff27b306c8077cd4f9ce1b31233c690a93ebc38a1b737a9ea"
        )
        self.assertEqual(expected_hash, FileHasher.get_artifact_hash(artifacts))
        self.assertEqual(
            expected_hash, FileHasher.get_artifact_hash(reversed(artifacts))
        )

    @patch("pathlib.Path.home")
    def test_local_file_hash(self, home):
        home.return_value = Path(self.temp.name)

        self.assertEqual(
            "d78f8bb992a56a597f6c7a1fb918bb78271367eb",
            FileHasher.get_local_file_hash(f"{self.temp.name}/test"),
        )

    @patch("pathlib.Path.home")
    def test_local_file_hashed_only_once(self, home):
        home.return_value = Path(self.temp.name)
        hashlib.sha1 = Mock(side_effect=hashlib.sha1)

        hash1 = FileHasher.get_local_file_hash(f"{self.temp.name}/test")
        hash2 = FileHasher.get_local_file_hash(f"{self.temp.name}/test")

        self.assertEqual("d78f8bb992a56a597f6c7a1fb918bb78271367eb", hash1)
        self.assertEqual("d78f8bb992a56a597f6c7a1fb918bb78271367eb", hash2)
        self.assertEqual(1, hashlib.sha1.call_count)

    @patch("pathlib.Path.home")
    def test_local_file_hashed_update(self, home):
        home.return_value = Path(self.temp.name)
        hashlib.sha1 = Mock(side_effect=hashlib.sha1)

        hash1 = FileHasher.get_local_file_hash(f"{self.temp.name}/test")

        # Minimal change in modification time
        time.sleep(0.1)

        with open(f"{self.temp.name}/test", "wb") as handler:
            handler.write(b"\x01\x02\x03\x04")

        hash2 = FileHasher.get_local_file_hash(f"{self.temp.name}/test")

        self.assertEqual("d78f8bb992a56a597f6c7a1fb918bb78271367eb", hash1)
        self.assertEqual("12dada1fff4d4787ade3333147202c3b443e376f", hash2)
        self.assertEqual(2, hashlib.sha1.call_count)
