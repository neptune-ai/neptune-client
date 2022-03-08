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

# pylint: disable=protected-access
import os
import uuid
from abc import abstractmethod
from datetime import datetime

from mock import Mock, patch

from neptune.new import ANONYMOUS
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    MetadataInconsistency,
)
from neptune.new.internal.backends.api_model import (
    Attribute,
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock


@patch(
    "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
    new=lambda _, _uuid, _type: [Attribute(path="test", type=AttributeType.STRING)],
)
@patch("neptune.new.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class AbstractTablesTestMixin:
    expected_container_type = None

    @abstractmethod
    def get_table(self):
        pass

    @abstractmethod
    def get_table_entries(self, table):
        pass

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    @classmethod
    def setUp(cls) -> None:
        if PROJECT_ENV_NAME in os.environ:
            del os.environ[PROJECT_ENV_NAME]

    @staticmethod
    def build_attributes_leaderboard(now: datetime):
        attributes = []
        attributes.append(
            AttributeWithProperties(
                "run/state", AttributeType.RUN_STATE, Mock(value="idle")
            )
        )
        attributes.append(
            AttributeWithProperties("float", AttributeType.FLOAT, Mock(value=12.5))
        )
        attributes.append(
            AttributeWithProperties(
                "string", AttributeType.STRING, Mock(value="some text")
            )
        )
        attributes.append(
            AttributeWithProperties("datetime", AttributeType.DATETIME, Mock(value=now))
        )
        attributes.append(
            AttributeWithProperties(
                "float/series", AttributeType.FLOAT_SERIES, Mock(last=8.7)
            )
        )
        attributes.append(
            AttributeWithProperties(
                "string/series", AttributeType.STRING_SERIES, Mock(last="last text")
            )
        )
        attributes.append(
            AttributeWithProperties(
                "string/set", AttributeType.STRING_SET, Mock(values=["a", "b"])
            )
        )
        attributes.append(
            AttributeWithProperties(
                "git/ref",
                AttributeType.GIT_REF,
                Mock(commit=Mock(commitId="abcdef0123456789")),
            )
        )
        attributes.append(AttributeWithProperties("file", AttributeType.FILE, None))
        attributes.append(
            AttributeWithProperties("file/set", AttributeType.FILE_SET, None)
        )
        attributes.append(
            AttributeWithProperties("image/series", AttributeType.IMAGE_SERIES, None)
        )
        return attributes

    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    def test_get_table_as_pandas(self, search_leaderboard_entries):
        # given
        now = datetime.now()
        attributes = self.build_attributes_leaderboard(now)

        # and
        empty_entry = LeaderboardEntry(str(uuid.uuid4()), [])
        filled_entry = LeaderboardEntry(str(uuid.uuid4()), attributes)
        search_leaderboard_entries.return_value = [empty_entry, filled_entry]

        # when
        df = self.get_table().to_pandas()

        # then
        self.assertEqual("idle", df["run/state"][1])
        self.assertEqual(12.5, df["float"][1])
        self.assertEqual("some text", df["string"][1])
        self.assertEqual(now, df["datetime"][1])
        self.assertEqual(8.7, df["float/series"][1])
        self.assertEqual("last text", df["string/series"][1])
        self.assertEqual("a,b", df["string/set"][1])
        self.assertEqual("abcdef0123456789", df["git/ref"][1])

        with self.assertRaises(KeyError):
            self.assertTrue(df["file"])
        with self.assertRaises(KeyError):
            self.assertTrue(df["file/set"])
        with self.assertRaises(KeyError):
            self.assertTrue(df["image/series"])

    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    @patch.object(NeptuneBackendMock, "download_file")
    @patch.object(NeptuneBackendMock, "download_file_set")
    def test_get_table_as_table_entries(
        self,
        download_file_set,
        download_file,
        search_leaderboard_entries,
    ):
        # given
        exp_id = str(uuid.uuid4())
        now = datetime.now()
        attributes = self.build_attributes_leaderboard(now)

        # and
        search_leaderboard_entries.return_value = [LeaderboardEntry(exp_id, attributes)]

        # when
        table_entry = self.get_table_entries(table=self.get_table())[0]

        # then
        self.assertEqual("idle", table_entry["run/state"].get())
        self.assertEqual("idle", table_entry["run"]["state"].get())
        self.assertEqual(12.5, table_entry["float"].get())
        self.assertEqual("some text", table_entry["string"].get())
        self.assertEqual(now, table_entry["datetime"].get())
        self.assertEqual(8.7, table_entry["float/series"].get())
        self.assertEqual("last text", table_entry["string/series"].get())
        self.assertEqual({"a", "b"}, table_entry["string/set"].get())
        self.assertEqual("abcdef0123456789", table_entry["git/ref"].get())

        with self.assertRaises(MetadataInconsistency):
            table_entry["file"].get()
        with self.assertRaises(MetadataInconsistency):
            table_entry["file/set"].get()
        with self.assertRaises(MetadataInconsistency):
            table_entry["image/series"].get()

        table_entry["file"].download("some_directory")
        download_file.assert_called_with(
            container_id=exp_id,
            container_type=self.expected_container_type,
            path=["file"],
            destination="some_directory",
        )

        table_entry["file/set"].download("some_directory")
        download_file_set.assert_called_with(
            container_id=exp_id,
            container_type=self.expected_container_type,
            path=["file", "set"],
            destination="some_directory",
        )
