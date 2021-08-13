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
# pylint: disable=protected-access
import pathlib
import tempfile
from unittest.mock import Mock

from _pytest.monkeypatch import MonkeyPatch

from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.exceptions import NeptuneUnhandledArtifactTypeException
from neptune.new.internal.artifacts.types import ArtifactFileData, ArtifactDriver, ArtifactDriversMap
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.atoms.artifact import Artifact as ArtifactAttr

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestArtifact(TestAttributeBase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()

        self.exp = self._create_run()
        self.path = self._random_path()
        self.path_str = path_to_str(self.path)

        self.artifact_hash = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        self.artifact_files = [
            ArtifactFileData("fname.txt", "da39a3ee5e6b4b0d3255bfef95601890afd80709", "test", {}),
            ArtifactFileData("subdir/other.mp3", "da39a3ee5e6b4b0d3255bfef95601890afd80709", "test", {}),
        ]

        self.exp.set_attribute(self.path_str, Artifact(self.exp, self.path))
        self.exp._backend._runs[self.exp._uuid].set(self.path, ArtifactAttr(self.artifact_hash))
        self.exp._backend._artifacts[self.exp._project_uuid, self.artifact_hash] = self.artifact_files

        self._downloads = set()

        class TestArtifactDriver(ArtifactDriver):
            @classmethod
            def get_type(cls):
                return "test"

            @classmethod
            def matches(cls, path: str) -> bool:
                return False

            @classmethod
            def get_tracked_files(cls, path, namespace=None):
                return []

            @classmethod
            def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
                destination.touch()

        self.test_artifact_driver = TestArtifactDriver

    def tearDown(self):
        self.monkeypatch.undo()

    def test_fetch_hash(self):
        fetched_hash = self.exp[self.path_str].fetch_hash()
        self.assertEqual(self.artifact_hash, fetched_hash)

    def test_fetch_files_list(self):
        fetched_hash = self.exp[self.path_str].fetch_files_list()
        self.assertEqual(self.artifact_files, fetched_hash)

    def test_download(self):
        self.monkeypatch.setattr(ArtifactDriversMap, 'match_type', Mock(return_value=self.test_artifact_driver))

        with tempfile.TemporaryDirectory() as temporary:
            self.exp[self.path_str].download(temporary)
            temporary_path = pathlib.Path(temporary)

            self.assertTrue((temporary_path / "fname.txt").exists())
            self.assertTrue((temporary_path / "subdir" / "other.mp3").exists())

    def test_download_unknown_type(self):
        self.monkeypatch.setattr(
            ArtifactDriversMap, "match_type", Mock(side_effect=NeptuneUnhandledArtifactTypeException("test")))

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(NeptuneUnhandledArtifactTypeException):
                self.exp[self.path_str].download(temporary)

            temporary_path = pathlib.Path(temporary)
            contents = list(temporary_path.iterdir())
            self.assertListEqual(contents, [])

    def test_track_files_to_new(self):
        # FIXME: test after implementing
        ...

    def test_track_files_to_existing(self):
        # FIXME: test after implementing
        ...
