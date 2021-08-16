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

from neptune.new.attributes.atoms.artifact_hash import ArtifactHash, ArtifactHashVal
from neptune.new.internal.operation import AssignArtifactHash
from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestArtifactHash(TestAttributeBase):

    def test_assign(self):
        value_and_expected = [
            ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
             "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            (ArtifactHashVal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
             "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ]

        for value, expected in value_and_expected:
            processor = MagicMock()
            exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
            var = ArtifactHash(exp, path)
            var.assign(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(AssignArtifactHash(path, expected), wait)

    def test_assign_type_error(self):
        values = ["foo", 10, None]
        for value in values:
            with self.assertRaises(Exception):
                ArtifactHash(MagicMock(), MagicMock()).assign(value)

    def test_get(self):
        exp, path = self._create_run(), self._random_path()
        var = ArtifactHash(exp, path)
        var.assign("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.assertEqual("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", var.fetch())
