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
import datetime
import tempfile
import unittest
from pathlib import Path

import boto3
import freezegun
from moto import mock_s3

from neptune.new.exceptions import NeptuneUnsupportedArtifactFunctionalityException
from neptune.new.internal.artifacts.types import (
    ArtifactDriversMap,
    ArtifactFileData,
    ArtifactFileType,
)
from neptune.new.internal.artifacts.drivers.s3 import S3ArtifactDriver

from tests.neptune.new.internal.artifacts.utils import md5


@mock_s3
class TestS3ArtifactDrivers(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "kuiper_belt"
        self.s3 = boto3.client("s3")
        self.s3.create_bucket(Bucket=self.bucket_name)
        self.update_time = datetime.datetime(2021, 5, 23, 3, 55, 26)
        with freezegun.freeze_time(self.update_time):
            self.s3.put_object(
                Bucket=self.bucket_name, Key="path/to/file1", Body=b"\xde\xad\xbe\xef"
            )
            self.s3.put_object(
                Bucket=self.bucket_name, Key="path/to/file2", Body=b"\x20"
            )
            self.s3.put_object(Bucket=self.bucket_name, Key="path/file3", Body=b"\x21")

    def test_match_by_path(self):
        self.assertEqual(
            ArtifactDriversMap.match_path(f"s3://{self.bucket_name}/path/to/"),
            S3ArtifactDriver,
        )

    def test_match_by_type(self):
        self.assertEqual(ArtifactDriversMap.match_type("S3"), S3ArtifactDriver)

    def test_file_download(self):
        artifact_file = ArtifactFileData(
            file_path="to/file1",
            file_hash="2f249230a8e7c2bf6005ccd2679259ec",
            type=ArtifactFileType.S3.value,
            metadata={"location": f"s3://{self.bucket_name}/path/to/file1"},
        )

        with tempfile.TemporaryDirectory() as temporary:
            local_destination = Path(temporary) / "target.txt"

            S3ArtifactDriver.download_file(
                destination=local_destination, file_definition=artifact_file
            )

            self.assertEqual("2f249230a8e7c2bf6005ccd2679259ec", md5(local_destination))

    def test_single_retrieval(self):
        files = S3ArtifactDriver.get_tracked_files(
            f"s3://{self.bucket_name}/path/to/file1"
        )

        self.assertEqual(1, len(files))
        self.assertIsInstance(files[0], ArtifactFileData)
        self.assertEqual(ArtifactFileType.S3.value, files[0].type)
        self.assertEqual("2f249230a8e7c2bf6005ccd2679259ec", files[0].file_hash)
        self.assertEqual("file1", files[0].file_path)
        self.assertEqual(4, files[0].size)
        self.assertEqual({"location", "last_modified"}, files[0].metadata.keys())
        self.assertEqual(
            f"s3://{self.bucket_name}/path/to/file1", files[0].metadata["location"]
        )
        self.assertEqual(
            self.update_time.strftime(S3ArtifactDriver.DATETIME_FORMAT),
            files[0].metadata["last_modified"],
        )

    def test_multiple_retrieval(self):
        files = S3ArtifactDriver.get_tracked_files(f"s3://{self.bucket_name}/path/to/")
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(2, len(files))

        self.assertEqual("2f249230a8e7c2bf6005ccd2679259ec", files[0].file_hash)
        self.assertEqual("file1", files[0].file_path)
        self.assertEqual(
            f"s3://{self.bucket_name}/path/to/file1", files[0].metadata["location"]
        )

        self.assertEqual("7215ee9c7d9dc229d2921a40e899ec5f", files[1].file_hash)
        self.assertEqual("file2", files[1].file_path)
        self.assertEqual(
            f"s3://{self.bucket_name}/path/to/file2", files[1].metadata["location"]
        )

    def test_multiple_retrieval_prefix(self):
        files = S3ArtifactDriver.get_tracked_files(
            f"s3://{self.bucket_name}/path/", "my/custom_path"
        )
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(len(files), 3)

        self.assertEqual("9033e0e305f247c0c3c80d0c7848c8b3", files[0].file_hash)
        self.assertEqual("my/custom_path/file3", files[0].file_path)
        self.assertEqual(
            f"s3://{self.bucket_name}/path/file3", files[0].metadata["location"]
        )

        self.assertEqual("2f249230a8e7c2bf6005ccd2679259ec", files[1].file_hash)
        self.assertEqual("my/custom_path/to/file1", files[1].file_path)
        self.assertEqual(
            f"s3://{self.bucket_name}/path/to/file1", files[1].metadata["location"]
        )

        self.assertEqual("7215ee9c7d9dc229d2921a40e899ec5f", files[2].file_hash)
        self.assertEqual("my/custom_path/to/file2", files[2].file_path)
        self.assertEqual(
            f"s3://{self.bucket_name}/path/to/file2", files[2].metadata["location"]
        )

    def test_wildcards_not_supported(self):
        with self.assertRaises(NeptuneUnsupportedArtifactFunctionalityException):
            S3ArtifactDriver.get_tracked_files(f"s3://{self.bucket_name}/*/to/")
