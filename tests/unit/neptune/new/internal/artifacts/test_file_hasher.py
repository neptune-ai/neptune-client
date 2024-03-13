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
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from mock import (
    Mock,
    patch,
)

from neptune.internal.artifacts.file_hasher import FileHasher
from neptune.internal.artifacts.types import ArtifactFileData


class TestFileHasher:
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

        expected_hash = "56e64245b1d4915ff27b306c8077cd4f9ce1b31233c690a93ebc38a1b737a9ea"
        assert expected_hash == FileHasher.get_artifact_hash(artifacts)
        assert expected_hash == FileHasher.get_artifact_hash(reversed(artifacts))

    def test_artifact_hash_without_metadata(self):
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

        expected_hash = "e6d96bccc12db43acc6e24e2e79052ecaee52307470e44f93d74ecfebc119128"
        assert expected_hash == FileHasher.get_artifact_hash_without_metadata(artifacts)
        assert expected_hash == FileHasher.get_artifact_hash_without_metadata(reversed(artifacts))

    @patch("pathlib.Path.home")
    def test_local_file_hash(self, home):
        with TemporaryDirectory() as tmp_dir:
            with open(f"{tmp_dir}/test", "wb") as handler:
                handler.write(b"\xde\xad\xbe\xef")

            home.return_value = Path(tmp_dir)

            assert "d78f8bb992a56a597f6c7a1fb918bb78271367eb" == FileHasher.get_local_file_hash(f"{tmp_dir}/test")

    @patch("pathlib.Path.home")
    def test_local_file_hashed_only_once(self, home):
        with TemporaryDirectory() as tmp_dir:
            with open(f"{tmp_dir}/test", "wb") as handler:
                handler.write(b"\xde\xad\xbe\xef")

            home.return_value = Path(tmp_dir)
            hashlib.sha1 = Mock(side_effect=hashlib.sha1)

            hash1 = FileHasher.get_local_file_hash(f"{tmp_dir}/test")
            hash2 = FileHasher.get_local_file_hash(f"{tmp_dir}/test")

            assert "d78f8bb992a56a597f6c7a1fb918bb78271367eb" == hash1
            assert "d78f8bb992a56a597f6c7a1fb918bb78271367eb" == hash2
            assert 1 == hashlib.sha1.call_count

    @patch("pathlib.Path.home")
    def test_local_file_hashed_update(self, home):
        with TemporaryDirectory() as tmp_dir:
            with open(f"{tmp_dir}/test", "wb") as handler:
                handler.write(b"\xde\xad\xbe\xef")

            home.return_value = Path(tmp_dir)
            hashlib.sha1 = Mock(side_effect=hashlib.sha1)

            hash1 = FileHasher.get_local_file_hash(f"{tmp_dir}/test")

            # Minimal change in modification time
            time.sleep(0.1)

            with open(f"{tmp_dir}/test", "wb") as handler:
                handler.write(b"\x01\x02\x03\x04")

            hash2 = FileHasher.get_local_file_hash(f"{tmp_dir}/test")

            assert "d78f8bb992a56a597f6c7a1fb918bb78271367eb" == hash1
            assert "12dada1fff4d4787ade3333147202c3b443e376f" == hash2
            assert 2 == hashlib.sha1.call_count
