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
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from neptune.new.exceptions import NeptuneLocalStorageAccessException
from neptune.new.internal.artifacts.drivers.local import LocalArtifactDriver
from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactFileData, ArtifactFileType
from tests.neptune.new.internal.artifacts.utils import md5


class TestLocalArtifactDrivers(unittest.TestCase):
    test_dir = None

    def setUp(self):
        self.test_dir = Path(str(tempfile.mktemp()))
        test_source_data = Path(__file__).parent.parent.parent.parent.parent.parent / 'data/local_artifact_drivers_data'
        test_data = self.test_dir / 'data'

        # copy source data
        shutil.copytree(test_source_data / 'files_to_track', test_data)

        # symbolic and hard link files
        # `link_to` is new in python 3.8
        # (test_source_data / 'file_to_link.txt').link_to(test_data / 'hardlinked_file.txt')
        os.link(
            src=(test_source_data / 'file_to_link.txt').as_posix(),
            dst=(test_data / 'hardlinked_file.txt').as_posix()
        )
        (test_data / 'symlinked_file.txt').symlink_to(test_source_data / 'file_to_link.txt')

        # symlink dir - content of this file won't be discovered
        (test_data / 'symlinked_dir').symlink_to(test_source_data / 'dir_to_link', target_is_directory=True)

    def tearDown(self) -> None:
        # clean tmp directory
        shutil.rmtree(self.test_dir.as_posix(), ignore_errors=True)

    def test_match_by_path(self):
        self.assertEqual(
            ArtifactDriversMap.match_path('file:///path/to/'),
            LocalArtifactDriver
        )
        self.assertEqual(
            ArtifactDriversMap.match_path('/path/to/'),
            LocalArtifactDriver
        )

    def test_match_by_type(self):
        self.assertEqual(
            ArtifactDriversMap.match_type('Local'),
            LocalArtifactDriver
        )

    def test_file_download(self):
        path = (self.test_dir / 'data/file1.txt').as_posix()
        artifact_file = ArtifactFileData(
            file_path=path,
            file_hash='??',
            type='??',
            metadata={}
        )

        with tempfile.TemporaryDirectory() as temporary:
            downloaded_file = Path(temporary) / 'downloaded_file.ext'

            LocalArtifactDriver.download_file(
                destination=downloaded_file,
                file_definition=artifact_file
            )

            self.assertTrue(Path(downloaded_file).is_symlink())
            self.assertEqual('6d615241ff583a4b67e14a4448aa08b6', md5(downloaded_file))

    def test_non_existing_file_download(self):
        path = '/wrong/path'
        artifact_file = ArtifactFileData(
            file_path=path,
            file_hash='??',
            type='??',
            metadata={}
        )

        with self.assertRaises(NeptuneLocalStorageAccessException), tempfile.TemporaryDirectory() as temporary:
            local_destination = Path(temporary)
            LocalArtifactDriver.download_file(
                destination=local_destination,
                file_definition=artifact_file
            )

    def test_single_retrieval(self):
        files = LocalArtifactDriver.get_tracked_files((self.test_dir / 'data/file1.txt').as_posix())

        self.assertEqual(1, len(files))
        self.assertIsInstance(files[0], ArtifactFileData)
        self.assertEqual(ArtifactFileType.LOCAL.value, files[0].type)
        self.assertEqual('72fae1be9ff9c1d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual('file1.txt', files[0].file_path)
        self.assertEqual(22, files[0].size)
        self.assertEqual(
            {'file_path', 'last_modified'},
            files[0].metadata.keys()
        )
        self.assertEqual(
            f"file://{(self.test_dir.resolve() / 'data/file1.txt').as_posix()}",
            files[0].metadata['file_path']
        )
        self.assertIsInstance(
            files[0].metadata['last_modified'],
            str
        )

    def test_multiple_retrieval(self):
        files = LocalArtifactDriver.get_tracked_files((self.test_dir / 'data').as_posix())
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(4, len(files))

        self.assertEqual('file1.txt', files[0].file_path)
        self.assertEqual('72fae1be9ff9c1d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual(22, files[0].size)
        self.assertEqual(
            f"file://{(self.test_dir.resolve() / 'data/file1.txt').as_posix()}",
            files[0].metadata['file_path'])

        self.assertEqual('hardlinked_file.txt', files[1].file_path)
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[1].file_hash)
        self.assertEqual(46, files[1].size)
        self.assertEqual(f"file://{(self.test_dir.resolve() / 'data/hardlinked_file.txt').as_posix()}",
                         files[1].metadata['file_path'])

        self.assertEqual('sub_dir/file_in_subdir.txt', files[2].file_path)
        self.assertEqual('66ac94061f0932fcb1954df995477cdcbb6b70b0', files[2].file_hash)
        self.assertEqual(25, files[2].size)
        self.assertEqual(f"file://{(self.test_dir.resolve() / 'data/sub_dir/file_in_subdir.txt').as_posix()}",
                         files[2].metadata['file_path'])

        self.assertEqual('symlinked_file.txt', files[3].file_path)
        metadata_path_suffix = 'neptune-client/tests/data/local_artifact_drivers_data/file_to_link.txt'
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[3].file_hash)
        self.assertEqual(46, files[3].size)
        self.assertTrue(files[3].metadata['file_path'].endswith(metadata_path_suffix))

    def test_multiple_retrieval_prefix(self):
        files = LocalArtifactDriver.get_tracked_files((self.test_dir / 'data').as_posix(), 'my/custom_path')
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(4, len(files))

        self.assertEqual('my/custom_path/file1.txt', files[0].file_path)
        self.assertEqual('72fae1be9ff9c1d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual(22, files[0].size)
        self.assertEqual(
            f"file://{(self.test_dir.resolve() / 'data/file1.txt').as_posix()}",
            files[0].metadata['file_path'])

        self.assertEqual('my/custom_path/hardlinked_file.txt', files[1].file_path)
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[1].file_hash)
        self.assertEqual(46, files[1].size)
        self.assertEqual(f"file://{(self.test_dir.resolve() / 'data/hardlinked_file.txt').as_posix()}",
                         files[1].metadata['file_path'])

        self.assertEqual('my/custom_path/sub_dir/file_in_subdir.txt', files[2].file_path)
        self.assertEqual('66ac94061f0932fcb1954df995477cdcbb6b70b0', files[2].file_hash)
        self.assertEqual(25, files[2].size)
        self.assertEqual(f"file://{(self.test_dir.resolve() / 'data/sub_dir/file_in_subdir.txt').as_posix()}",
                         files[2].metadata['file_path'])

        self.assertEqual('my/custom_path/symlinked_file.txt', files[3].file_path)
        metadata_path_suffix = 'neptune-client/tests/data/local_artifact_drivers_data/file_to_link.txt'
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[3].file_hash)
        self.assertEqual(46, files[3].size)
        self.assertTrue(files[3].metadata['file_path'].endswith(metadata_path_suffix))
