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

from mock import patch

from neptune.internal.storage.storage_utils import UploadEntry, UploadPackage, split_upload_files


class TestUploadStorageUtils(unittest.TestCase):
    MAX_PACKAGE_SIZE = 1024

    @patch('os.path.isdir', new=lambda _: False)
    @patch('os.path.getsize')
    def test_split_upload_files_should_generate_upload_files_list_for_only_one_file(self, getsize):
        # GIVEN
        entry = UploadEntry("/tmp/test.gz", "test.gz")
        # AND
        file_entry = (entry.source_path, entry.target_path)
        size = 10 * self.MAX_PACKAGE_SIZE
        getsize.return_value = size

        # EXPECT
        expected = UploadPackage()
        expected.update(entry, size)
        self.assertEqual(list(split_upload_files([file_entry], max_package_size=self.MAX_PACKAGE_SIZE)), [expected])

    @patch('os.path.isdir', new=lambda _: False)
    @patch('os.path.getsize')
    def test_split_upload_files_should_not_generate_empty_packages(self, getsize):
        # GIVEN
        entry = UploadEntry("/tmp/test.gz", "test.gz")
        # AND
        file_entry = (entry.source_path, entry.target_path)
        size = 10 * self.MAX_PACKAGE_SIZE
        getsize.return_value = size

        # EXPECT
        expected = UploadPackage()
        expected.update(entry, size)
        for package in split_upload_files([file_entry], max_package_size=self.MAX_PACKAGE_SIZE):
            self.assertFalse(package.is_empty())
