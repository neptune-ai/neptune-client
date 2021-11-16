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

from neptune.new.internal.artifacts.types import ArtifactMetadataSerializer


class TestArtifactMetadataSerializer(unittest.TestCase):
    def test_simple(self):
        metadata = {
            "location": "s3://bucket/path/to/file",
            "last_modification": "2021-08-09 09:41:53",
            "file_size": "18",
        }

        serialized = ArtifactMetadataSerializer.serialize(metadata)

        self.assertListEqual(
            [
                {"key": "file_size", "value": "18"},
                {"key": "last_modification", "value": "2021-08-09 09:41:53"},
                {"key": "location", "value": "s3://bucket/path/to/file"},
            ],
            serialized,
        )

        deserialized = ArtifactMetadataSerializer.deserialize(serialized)

        self.assertDictEqual(metadata, deserialized)
