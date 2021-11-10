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


from mock import MagicMock

from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.internal.operation import AssignArtifact
from neptune.new.types.atoms.artifact import Artifact as ArtifactVal
from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestArtifactHash(TestAttributeBase):
    def test_assign_type_error(self):
        values = ["foo", 10, None]
        for value in values:
            with self.assertRaises(Exception):
                Artifact(MagicMock(), MagicMock()).assign(value)

    def test_fetch(self):
        exp, path = self._create_run(), self._random_path()
        var = Artifact(exp, path)
        var._enqueue_operation(
            AssignArtifact(
                var._path,
                ArtifactVal(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                ).hash,
            ),
            False,
        )
        self.assertEqual(
            ArtifactVal(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            ),
            var.fetch(),
        )

    def test_fetch_hash(self):
        exp, path = self._create_run(), self._random_path()
        var = Artifact(exp, path)
        var._enqueue_operation(
            AssignArtifact(
                var._path,
                ArtifactVal(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                ).hash,
            ),
            False,
        )
        self.assertEqual(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            var.fetch_hash(),
        )
