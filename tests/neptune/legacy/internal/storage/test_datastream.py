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

import unittest

import pytest
from mock import Mock

from neptune.common.exceptions import InternalClientError
from neptune.common.storage.datastream import FileChunker
from neptune.legacy.internal.api_clients.client_config import MultipartConfig


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
        chunker = FileChunker(Mock(), Mock(), total_size=file_size, multipart_config=self.multipart_config)

        chunk_size = chunker._get_chunk_size()

        chunk_count = self.get_chunk_count(file_size, chunk_size)
        assert chunk_count == expected_chunk_count
        assert chunk_size == expected_chunk_size
        assert chunk_count <= self.multipart_config.max_chunk_count
        assert chunk_size <= self.multipart_config.max_chunk_size
        assert chunk_size >= self.multipart_config.min_chunk_size

    def test_too_large_file(self):
        file_size = 1_073_741_824_001

        chunker = FileChunker(Mock(), Mock(), total_size=file_size, multipart_config=self.multipart_config)

        with pytest.raises(InternalClientError):
            chunker._get_chunk_size()


if __name__ == "__main__":
    unittest.main()
