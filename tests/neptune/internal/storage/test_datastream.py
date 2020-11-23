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

from io import StringIO
from mock import patch

from neptune.internal.storage.datastream import FileChunkStream, FileChunk
from neptune.internal.storage.storage_utils import UploadEntry


class TestFileChunkStream(unittest.TestCase):

    @patch('os.path.exists', new=lambda _: True)
    @patch('stat.S_ISDIR', new=lambda _: False)
    @patch('os.lstat')
    def test_permissions_to_unix_string_for_file(self, lstat):
        # given
        lstat.return_value.st_mode = 0o731

        # when
        permissions_string = FileChunkStream.permissions_to_unix_string('/some/path')

        # then
        self.assertEqual('-rwx-wx--x', permissions_string)

    @patch('os.path.exists', new=lambda _: True)
    @patch('stat.S_ISDIR', new=lambda _: True)
    @patch('os.lstat')
    def test_permissions_to_unix_string_for_directory(self, lstat):
        # given
        lstat.return_value.st_mode = 0o642

        # when
        permissions_string = FileChunkStream.permissions_to_unix_string('/some/path')

        # then
        self.assertEqual('drw-r---w-', permissions_string)

    @patch('os.path.exists', new=lambda _: False)
    def test_permissions_to_unix_string_for_nonexistent_file(self):
        # when
        permissions_string = FileChunkStream.permissions_to_unix_string('/some/path')

        # then
        self.assertEqual('-' * 10, permissions_string)

    def test_generate_chunks_from_stream(self):
        # given
        text = u"ABCDEFGHIJKLMNOPRSTUWXYZ"

        # when
        stream = FileChunkStream(UploadEntry(StringIO(text), "some/path"))
        chunks = list()
        for chunk in stream.generate(chunk_size=10):
            chunks.append(chunk)

        # then
        self.assertEqual(stream.length, None)
        self.assertEqual(chunks, [
            FileChunk(b"ABCDEFGHIJ", 0, 10),
            FileChunk(b"KLMNOPRSTU", 10, 20),
            FileChunk(b"WXYZ", 20, 24)
        ])


if __name__ == '__main__':
    unittest.main()
