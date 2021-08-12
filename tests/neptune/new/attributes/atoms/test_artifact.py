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

from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.atoms.artifact import Artifact as ArtifactAttr

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestArtifact(TestAttributeBase):
    # pylint: disable=protected-access
    def setUp(self):
        self.exp = self._create_run()
        self.path = self._random_path()
        self.path_str = path_to_str(self.path)

        self.artifact_hash = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        self.artifact_files = []

        self.exp.set_attribute(self.path_str, Artifact(self.exp, self.path))
        self.exp._backend._runs[self.exp._uuid].set(self.path, ArtifactAttr(self.artifact_hash))
        self.exp._backend._artifacts[self.exp._project_uuid, self.artifact_hash] = []

    def test_fetch_hash(self):
        fetched_hash = self.exp[self.path_str].fetch_hash()
        self.assertEqual(self.artifact_hash, fetched_hash)

    def test_fetch_files_list(self):
        fetched_hash = self.exp[self.path_str].fetch_files_list()
        self.assertEqual(self.artifact_files, fetched_hash)

    def test_download(self):
        pass

    def test_download_unknown_type(self):
        pass

    def test_track_files_to_new(self):
        # FIXME: test after implementing
        ...

    def test_track_files_to_existing(self):
        # FIXME: test after implementing
        ...
