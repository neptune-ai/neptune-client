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

from mock import patch, MagicMock

from neptune.internal.storage.storage_utils \
    import UploadEntry, UploadPackage, split_upload_files, upload_to_storage


class TestUploadStorageUtils(unittest.TestCase):
    MAX_PACKAGE_SIZE = 1024

    @patch('os.path.isdir', new=lambda _: False)
    @patch('os.path.getsize')
    def test_split_upload_files_should_generate_upload_files_list_for_only_one_file(self, getsize):
        # GIVEN
        entry = UploadEntry("/tmp/test.gz", "test.gz")
        size = 10 * self.MAX_PACKAGE_SIZE
        getsize.return_value = size

        # EXPECT
        expected = UploadPackage()
        expected.update(entry, size)
        self.assertEqual(list(split_upload_files([entry], max_package_size=self.MAX_PACKAGE_SIZE)), [expected])

    @patch('os.path.isdir', new=lambda _: False)
    @patch('os.path.getsize')
    def test_split_upload_files_should_not_generate_empty_packages(self, getsize):
        # GIVEN
        entry = UploadEntry("/tmp/test.gz", "test.gz")
        # AND
        upload_entry = UploadEntry(entry.source_path, entry.target_path)
        size = 10 * self.MAX_PACKAGE_SIZE
        getsize.return_value = size

        # EXPECT
        expected = UploadPackage()
        expected.update(entry, size)
        for package in split_upload_files([upload_entry], max_package_size=self.MAX_PACKAGE_SIZE):
            self.assertFalse(package.is_empty())

    @patch('io.open', new=MagicMock)
    @patch('os.path.getsize', new=lambda path: 101 * 1024 * 1024)
    @patch('neptune.internal.storage.storage_utils._logger.warning')
    def test_upload_large_sources_should_generate_warning(self, warning):
        # GIVEN
        entry = UploadEntry("/tmp/mocked/file", "some_file")

        # WHEN
        upload_to_storage(upload_entries=[entry],
                          upload_api_fun=MagicMock(),
                          upload_tar_api_fun=MagicMock(),
                          warn_limit=100 * 1024 * 1024)

        # THEN
        warning.assert_any_call('You are sending %dMB of source code to Neptune. '
                                'It is pretty uncommon - please make sure it\'s what you wanted.', 101)
        warning.assert_any_call('%d MB (100%%) of source code was sent to Neptune.', 101)

    @patch('io.open', new=MagicMock)
    @patch('os.path.getsize', new=lambda path: 99 * 1024 * 1024)
    @patch('neptune.internal.storage.storage_utils._logger.warning')
    def test_upload_small_sources_should_not_generate_warning(self, warning):
        # GIVEN
        entry = UploadEntry("/tmp/mocked/file", "some_file")

        # WHEN
        upload_to_storage(upload_entries=[entry],
                          upload_api_fun=MagicMock(),
                          upload_tar_api_fun=MagicMock(),
                          warn_limit=100 * 1024 * 1024)

        # THEN
        warning.assert_not_called()
