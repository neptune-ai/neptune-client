#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import skip

from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactFileData, ArtifactFileType
from neptune.new.internal.artifacts.drivers.local import LocalArtifactDriver

from tests.neptune.new.internal.artifacts.utils import append_non_relative_path, md5


class TestLocalArtifactDrivers(unittest.TestCase):
    test_dir = None

    def setUp(self):
        self.test_dir = Path(str(tempfile.mktemp()))
        print(self.test_dir)
        test_source_data = Path(__file__).parent.parent.parent.parent.parent.parent / 'data/local_artifact_drivers_data'
        test_data = self.test_dir / 'data'

        # copy source data
        shutil.copytree(test_source_data / 'files_to_track', test_data)

        # link file and dir
        (test_source_data / 'file_to_link.txt').link_to(test_data / 'hardlinked_file.txt')
        (test_data / 'symlinked_dir').symlink_to(test_source_data / 'dir_to_link', target_is_directory=True)
        (test_data / 'symlinked_file.txt').symlink_to(test_source_data / 'file_to_link.txt')

    def tearDown(self) -> None:
        # clean tmp directory
        return
        shutil.rmtree(self.test_dir.as_posix(), ignore_errors=True)

    def test_match_by_path(self):
        self.assertEqual(
            ArtifactDriversMap.match_path(f'file:///path/to/'),
            LocalArtifactDriver
        )
        self.assertEqual(
            ArtifactDriversMap.match_path(f'/path/to/'),
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

        with tempfile.TemporaryDirectory('foto') as temporary:
            local_destination = Path(temporary)

            LocalArtifactDriver.download_file(
                destination=local_destination,
                file_definition=artifact_file
            )

            downloaded_file = append_non_relative_path(local_destination, path)
            self.assertEqual('6d615241ff583a4b67e14a4448aa08b6', md5(downloaded_file))

    def test_single_retrieval(self):
        files = LocalArtifactDriver.get_tracked_files(
            (self.test_dir / 'data/file1.txt').as_posix()
        )

        path = (self.test_dir / 'data/file1.txt').as_posix()
        self.assertEqual(1, len(files))
        self.assertIsInstance(files[0], ArtifactFileData)
        self.assertEqual(ArtifactFileType.LOCAL.value, files[0].type)
        self.assertEqual('72fae1be9ff9c1d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual(path, files[0].file_path)
        self.assertEqual(
            {'file_path', 'file_size', 'last_modified'},
            files[0].metadata.keys()
        )
        self.assertEqual(
            f'file://{path}',
            files[0].metadata['file_path']
        )
        self.assertIsInstance(
            files[0].metadata['last_modified'],
            datetime.datetime
        )
        self.assertEqual(22, files[0].metadata['file_size'])

    def test_multiple_retrieval(self):
        files = LocalArtifactDriver.get_tracked_files(
            (self.test_dir / 'data').as_posix()
        )
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(4, len(files))

        path0 = (self.test_dir / 'data/file1.txt').as_posix()
        self.assertEqual('72fae1be9ff9c1d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual(path0, files[0].file_path)
        self.assertEqual(f'file://{path0}', files[0].metadata['file_path'])
        self.assertEqual(22, files[0].metadata['file_size'])

        path1 = (self.test_dir / 'data/hardlinked_file.txt').as_posix()
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[1].file_hash)
        self.assertEqual(path1, files[1].file_path)
        self.assertEqual(f'file://{path1}', files[1].metadata['file_path'])
        self.assertEqual(46, files[1].metadata['file_size'])

        path2 = (self.test_dir / 'data/sub_dir/file_in_subdir.txt').as_posix()
        self.assertEqual('66ac94061f0932fcb1954df995477cdcbb6b70b0', files[2].file_hash)
        self.assertEqual(path2, files[2].file_path)
        self.assertEqual(f'file://{path2}', files[2].metadata['file_path'])
        self.assertEqual(25, files[2].metadata['file_size'])

        path3 = (self.test_dir / 'data/symlinked_file.txt').as_posix()
        self.assertEqual('78f994e9b118aedbb5206ab83f6706e01f1c1bb5', files[3].file_hash)
        self.assertEqual(path3, files[3].file_path)
        self.assertEqual(f'file://{path3}', files[3].metadata['file_path'])
        self.assertEqual(46, files[3].metadata['file_size'])

    @skip('not implemented yet')
    def test_multiple_retrieval_prefix(self):
        files = LocalArtifactDriver.get_tracked_files(
            (self.test_dir / 'data').as_posix(),
            'my/custom_path'
        )
        files = sorted(files, key=lambda file: file.file_path)

        self.assertEqual(4, len(files))

        custom_path0 = (self.test_dir / 'my/custom_path/data/file2.txt').as_posix()
        path0 = (self.test_dir / 'data/file2.txt').as_posix()
        self.assertEqual('72fae2be9ff9c2d5fd7a0d97977bba9cc96d702d', files[0].file_hash)
        self.assertEqual(custom_path0, files[0].file_path)
        self.assertEqual(f'file://{path0}', files[0].metadata['file_path'])

        path2 = (self.test_dir / 'data/linked_file.txt').as_posix()
        self.assertEqual('78f994e9b228aedbb5206ab83f6706e02f2c2bb5', files[1].file_hash)
        self.assertEqual(path2, files[1].file_path)
        self.assertEqual(f'file://{path2}', files[1].metadata['file_path'])

        path2 = (self.test_dir / 'data/sub_dir/file_in_subdir.txt').as_posix()
        self.assertEqual('66ac94062f0932fcb2954df995477cdcbb6b70b0', files[2].file_hash)
        self.assertEqual(path2, files[2].file_path)
        self.assertEqual(f'file://{path2}', files[2].metadata['file_path'])
