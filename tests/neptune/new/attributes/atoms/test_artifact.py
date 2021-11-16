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
import uuid
import pathlib
import tempfile
from unittest.mock import Mock
from mock import MagicMock, call

from _pytest.monkeypatch import MonkeyPatch

from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.exceptions import NeptuneUnhandledArtifactTypeException
from neptune.new.internal.artifacts.types import (
    ArtifactFileData,
    ArtifactDriver,
    ArtifactDriversMap,
)
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.atoms.artifact import Artifact as ArtifactAttr
from neptune.new.internal.operation import TrackFilesToArtifact

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestArtifact(TestAttributeBase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()

        self.wait = self._random_wait()
        self.op_processor = MagicMock()
        self.exp = self._create_run(processor=self.op_processor)
        self.path = self._random_path()
        self.path_str = path_to_str(self.path)

        self.artifact_hash = (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        self.artifact_files = [
            ArtifactFileData(
                file_path="fname.txt",
                file_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                type="test",
                size=213,
                metadata={},
            ),
            ArtifactFileData(
                file_path="subdir/other.mp3",
                file_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                type="test",
                metadata={},
            ),
        ]

        self.exp.set_attribute(self.path_str, Artifact(self.exp, self.path))
        self.exp._backend._runs[self.exp._id].set(
            self.path, ArtifactAttr(self.artifact_hash)
        )
        self.exp._backend._artifacts[
            self.exp._project_id, self.artifact_hash
        ] = self.artifact_files

        self._downloads = set()

        class TestArtifactDriver(ArtifactDriver):
            @classmethod
            def get_type(cls):
                return "test"

            @classmethod
            def matches(cls, path: str) -> bool:
                return False

            @classmethod
            def get_tracked_files(cls, path, destination=None):
                return []

            @classmethod
            def download_file(
                cls, destination: pathlib.Path, file_definition: ArtifactFileData
            ):
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
        self.monkeypatch.setattr(
            ArtifactDriversMap,
            "match_type",
            Mock(return_value=self.test_artifact_driver),
        )

        with tempfile.TemporaryDirectory() as temporary:
            self.exp[self.path_str].download(temporary)
            temporary_path = pathlib.Path(temporary)

            self.assertTrue((temporary_path / "fname.txt").exists())
            self.assertTrue((temporary_path / "subdir" / "other.mp3").exists())

    def test_download_unknown_type(self):
        self.monkeypatch.setattr(
            ArtifactDriversMap,
            "match_type",
            Mock(side_effect=NeptuneUnhandledArtifactTypeException("test")),
        )

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(NeptuneUnhandledArtifactTypeException):
                self.exp[self.path_str].download(temporary)

            temporary_path = pathlib.Path(temporary)
            contents = list(temporary_path.iterdir())
            self.assertListEqual(contents, [])

    def test_track_files_to_artifact(self):
        source_location = str(uuid.uuid4())
        destination = str(uuid.uuid4())
        source_location2 = str(uuid.uuid4())
        destination2 = str(uuid.uuid4())

        var = Artifact(self.exp, self.path)
        var.track_files(path=source_location, destination=destination, wait=self.wait)
        var.track_files(path=source_location2, destination=destination2, wait=self.wait)

        self.op_processor.enqueue_operation.assert_has_calls(
            [
                call(
                    TrackFilesToArtifact(
                        self.path,
                        self.exp._project_id,
                        [(source_location, destination)],
                    ),
                    self.wait,
                ),
                call(
                    TrackFilesToArtifact(
                        self.path,
                        self.exp._project_id,
                        [(source_location2, destination2)],
                    ),
                    self.wait,
                ),
            ]
        )
