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
import datetime

from neptune.new.internal.artifacts.file_hasher import ArtifactMetadataSerializer


class TestFileHasher(unittest.TestCase):
    def test_artifact_hash(self):
        metadata = {
            'location': "s3://bucket/path/to/file",
            'last_modification': datetime.datetime(2021, 8, 9, 9, 41, 53),
            'file_size': 18
        }

        serialized = ArtifactMetadataSerializer.serialize(metadata)

        self.assertListEqual(
            [
                ('file_size', '18'),
                ('last_modification', '2021-08-09 09:41:53'),
                ('location', 's3://bucket/path/to/file')
            ],
            serialized
        )
