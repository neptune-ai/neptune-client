#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

# pylint: disable=protected-access
import unittest
from io import BytesIO

import pytest
from mock import Mock, patch

from neptune.internal.api_clients.client_config import MultipartConfig
from neptune.internal.storage.datastream import FileChunk, FileChunker, FileChunkStream
from neptune.internal.storage.storage_utils import (
    AttributeUploadConfiguration,
    UploadEntry,
)
from neptune.new.exceptions import InternalClientError


class TestFileChunkStream(unittest.TestCase):
    @patch("os.path.exists", new=lambda _: True)
    @patch("stat.S_ISDIR", new=lambda _: False)
    @patch("os.lstat")
    def test_permissions_to_unix_string_for_file(self, lstat):
        # given
        lstat.return_value.st_mode = 0o731

        # when
        permissions_string = UploadEntry.permissions_to_unix_string("/some/path")

        # then
        self.assertEqual("-rwx-wx--x", permissions_string)

    @patch("os.path.exists", new=lambda _: True)
    @patch("stat.S_ISDIR", new=lambda _: True)
    @patch("os.lstat")
    def test_permissions_to_unix_string_for_directory(self, lstat):
        # given
        lstat.return_value.st_mode = 0o642

        # when
        permissions_string = UploadEntry.permissions_to_unix_string("/some/path")

        # then
        self.assertEqual("drw-r---w-", permissions_string)

    @patch("os.path.exists", new=lambda _: False)
    def test_permissions_to_unix_string_for_nonexistent_file(self):
        # when
        permissions_string = UploadEntry.permissions_to_unix_string("/some/path")

        # then
        self.assertEqual("-" * 10, permissions_string)

    def test_generate_chunks_from_stream(self):
        # given
        text = "ABCDEFGHIJKLMNOPRSTUWXYZ"

        # when
        stream = FileChunkStream(
            UploadEntry(BytesIO(bytes(text, "utf-8")), "some/path"),
            AttributeUploadConfiguration(10),
        )
        chunks = list()
        for chunk in stream.generate():
            chunks.append(chunk)

        # then
        self.assertEqual(stream.length, 24)
        self.assertEqual(
            chunks,
            [
                FileChunk(b"ABCDEFGHIJ", 0, 10),
                FileChunk(b"KLMNOPRSTU", 10, 20),
                FileChunk(b"WXYZ", 20, 24),
            ],
        )


class TestFileChunker:
    multipart_config = MultipartConfig(
        min_chunk_size=5_242_880,  # 5 MB
        max_chunk_size=1_073_741_824,  # 1 GB
        max_chunk_count=1_000,
        max_single_part_size=5_242_880,  # 1 GB
    )

    def get_chunk_count(self, file_size, chunk_size):
        chunk_idx = 0
        while file_size > chunk_size:
            chunk_idx += 1
            file_size -= chunk_size
        return chunk_idx + 1

    @pytest.mark.parametrize(
        "file_size, expected_chunk_size, expected_chunk_count",
        (
            (1_000_000, 5_242_880, 1),
            (6_000_000, 5_242_880, 2),
            (5_242_880_000, 5_242_880, 1_000),
            (5_242_880_001, 5_242_881, 1_000),
            (5_242_891_001, 5_242_892, 1_000),
            (1_073_741_824_000, 1_073_741_824, 1_000),
        ),
    )
    def test_chunk_size_for_small_file(self, file_size, expected_chunk_size, expected_chunk_count):
        chunker = FileChunker(
            Mock(), Mock(), total_size=file_size, multipart_config=self.multipart_config
        )

        chunk_size = chunker._get_chunk_size()

        chunk_count = self.get_chunk_count(file_size, chunk_size)
        assert chunk_count == expected_chunk_count
        assert chunk_size == expected_chunk_size
        assert chunk_count <= self.multipart_config.max_chunk_count
        assert chunk_size <= self.multipart_config.max_chunk_size
        assert chunk_size >= self.multipart_config.min_chunk_size

    def test_too_large_file(self):
        file_size = 1_073_741_824_001

        chunker = FileChunker(
            Mock(), Mock(), total_size=file_size, multipart_config=self.multipart_config
        )

        with pytest.raises(InternalClientError):
            chunker._get_chunk_size()


if __name__ == "__main__":
    unittest.main()
