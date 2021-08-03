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
import unittest
from urllib.parse import urlparse

from neptune.internal.artifacts.types import ArtifactDriversMap, ArtifactDriver
from neptune.new.exceptions import NeptuneUnhandledArtifactSchemeException, NeptuneUnhandledArtifactTypeException


class TestArtifactDriversMap(unittest.TestCase):
    # pylint:disable=protected-access

    def setUp(self):
        self._impl_backup = ArtifactDriversMap._implementations
        ArtifactDriversMap._implementations = []

        class TestArtifactDriver(ArtifactDriver):
            def get_type(self):
                return "test"

            def matches(self, path: str) -> bool:
                return urlparse(path).scheme == 'test'

        self.test_driver_instance = TestArtifactDriver()
        ArtifactDriversMap._implementations = [self.test_driver_instance]

    def tearDown(self):
        ArtifactDriversMap._implementations = self._impl_backup

    def test_driver_autoregister(self):
        class PkArtifactDriver(ArtifactDriver):
            def get_type(self):
                return "PK"

            def matches(self, path: str) -> bool:
                return urlparse(path).scheme == 'pk'

        for artifact_driver in ArtifactDriversMap._implementations:
            if isinstance(artifact_driver, PkArtifactDriver):
                break
        else:
            assert False, "PkArtifactDriver not registered with subclass logic"

    def test_match_by_path(self):
        driver_instance = ArtifactDriversMap.match_path("test://path/to/file")

        self.assertEqual(driver_instance, self.test_driver_instance)

    def test_unmatched_path_raises_exception(self):
        with self.assertRaises(NeptuneUnhandledArtifactSchemeException):
            ArtifactDriversMap.match_path("test2://path/to/file")

    def test_match_by_type(self):
        driver_instance = ArtifactDriversMap.match_type("test")

        self.assertEqual(driver_instance, self.test_driver_instance)

    def test_unmatched_type_raises_exception(self):
        with self.assertRaises(NeptuneUnhandledArtifactTypeException):
            ArtifactDriversMap.match_type("test2")


if __name__ == '__main__':
    unittest.main()
