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
import os
import uuid
from abc import abstractmethod
from datetime import datetime
from typing import List

import pytest
from mock import patch

from neptune import ANONYMOUS_API_TOKEN
from neptune.api.models import (
    DateTimeField,
    FieldDefinition,
    FieldType,
    FloatField,
    FloatSeriesField,
    LeaderboardEntry,
    ObjectStateField,
    StringField,
    StringSeriesField,
    StringSetField,
)
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.table import (
    Table,
    TableEntry,
)


@patch(
    "neptune.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
    new=lambda _, _uuid, _type: [FieldDefinition(path="test", type=FieldType.STRING)],
)
@patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class AbstractTablesTestMixin:
    expected_container_type = None

    @abstractmethod
    def get_table(self, **kwargs) -> Table:
        pass

    @abstractmethod
    def get_table_entries(self, table) -> List[TableEntry]:
        pass

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @classmethod
    def setUp(cls) -> None:
        if PROJECT_ENV_NAME in os.environ:
            del os.environ[PROJECT_ENV_NAME]

    @staticmethod
    def build_fields_leaderboard(now: datetime):
        return [
            ObjectStateField(path="run/state", value="Inactive"),
            FloatField(path="float", value=12.5),
            StringField(path="string", value="some text"),
            DateTimeField(path="datetime", value=now),
            FloatSeriesField(path="float/series", last=8.7),
            StringSeriesField(path="string/series", last="last text"),
            StringSetField(path="string/set", values={"a", "b"}),
        ]

    @pytest.mark.skip("Backend not implemented")
    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    def test_get_table_with_columns_filter(self, search_leaderboard_entries):
        # when
        self.get_table(columns=["datetime"])

        # then
        self.assertEqual(1, search_leaderboard_entries.call_count)
        parameters = search_leaderboard_entries.call_args[1]
        self.assertEqual({"sys/id", "sys/creation_time", "datetime"}, parameters.get("columns"))

    @pytest.mark.skip("Backend not implemented")
    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    def test_get_table_as_pandas(self, search_leaderboard_entries):
        # given
        now = datetime.now()
        fields = self.build_fields_leaderboard(now)

        # and
        empty_entry = LeaderboardEntry(object_id=str(uuid.uuid4()), fields=[])
        filled_entry = LeaderboardEntry(object_id=str(uuid.uuid4()), fields=fields)
        search_leaderboard_entries.return_value = [empty_entry, filled_entry]

        # when
        df = self.get_table().to_pandas()

        # then
        self.assertEqual("Inactive", df["run/state"][1])
        self.assertEqual(12.5, df["float"][1])
        self.assertEqual("some text", df["string"][1])
        self.assertEqual(now, df["datetime"][1])
        self.assertEqual(8.7, df["float/series"][1])
        self.assertEqual("last text", df["string/series"][1])
        self.assertEqual({"a", "b"}, set(df["string/set"][1].split(",")))

    @pytest.mark.skip("Backend not implemented")
    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    def test_get_table_as_rows(self, search_leaderboard_entries):
        # given
        now = datetime.now()
        fields = self.build_fields_leaderboard(now)

        # and
        empty_entry = LeaderboardEntry(object_id=str(uuid.uuid4()), fields=[])
        filled_entry = LeaderboardEntry(object_id=str(uuid.uuid4()), fields=fields)
        search_leaderboard_entries.return_value = [empty_entry, filled_entry]

        # and
        # (check if using both to_rows and table generator produces the same results)
        table_gen = self.get_table()
        next(table_gen)  # to move to the second table entry
        # when
        for row in (self.get_table().to_rows()[1], next(table_gen)):
            # then
            self.assertEqual("Inactive", row.get_attribute_value("run/state"))
            self.assertEqual(12.5, row.get_attribute_value("float"))
            self.assertEqual("some text", row.get_attribute_value("string"))
            self.assertEqual(now, row.get_attribute_value("datetime"))
            self.assertEqual(8.7, row.get_attribute_value("float/series"))
            self.assertEqual("last text", row.get_attribute_value("string/series"))
            self.assertEqual({"a", "b"}, row.get_attribute_value("string/set"))

    @pytest.mark.skip("Backend not implemented")
    @patch.object(NeptuneBackendMock, "search_leaderboard_entries")
    def test_get_table_as_table_entries(
        self,
        search_leaderboard_entries,
    ):
        # given
        exp_id = str(uuid.uuid4())
        now = datetime.now()
        fields = self.build_fields_leaderboard(now)

        # and
        search_leaderboard_entries.return_value = [LeaderboardEntry(object_id=exp_id, fields=fields)]

        # when
        table_entry = self.get_table_entries(table=self.get_table())[0]

        # then
        self.assertEqual("Inactive", table_entry["run/state"].get())
        self.assertEqual("Inactive", table_entry["run"]["state"].get())
        self.assertEqual(12.5, table_entry["float"].get())
        self.assertEqual("some text", table_entry["string"].get())
        self.assertEqual(now, table_entry["datetime"].get())
        self.assertEqual(8.7, table_entry["float/series"].get())
        self.assertEqual("last text", table_entry["string/series"].get())
        self.assertEqual({"a", "b"}, table_entry["string/set"].get())

    @pytest.mark.skip("Backend not implemented")
    def test_table_limit(self):
        with pytest.raises(ValueError):
            self.get_table(limit=-4)

        with pytest.raises(ValueError):
            self.get_table(limit=0)
