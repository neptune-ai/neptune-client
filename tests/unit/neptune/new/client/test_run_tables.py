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

import unittest
from datetime import datetime
from typing import List

from mock import patch

from neptune import init_project
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.internal.container_type import ContainerType
from neptune.metadata_containers.utils import DATE_FORMAT
from neptune.table import (
    Table,
    TableEntry,
)
from tests.unit.neptune.new.client.abstract_tables_test import AbstractTablesTestMixin


class TestRunTables(AbstractTablesTestMixin, unittest.TestCase):
    expected_container_type = ContainerType.RUN

    def get_table(self, **kwargs) -> Table:
        return init_project(project="organization/project", mode="read-only").fetch_runs_table(**kwargs)

    def get_table_entries(self, table) -> List[TableEntry]:
        return table.to_rows()

    @patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
    def test_fetch_runs_table_is_case_insensitive(self):
        states = ["active", "inactive", "Active", "Inactive", "aCTive", "INacTiVe"]
        for state in states:
            with self.subTest(state):
                try:
                    self.get_table(state=state)
                except Exception as e:
                    self.fail(e)

    @patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
    def test_fetch_runs_table_raises_correct_exception_for_incorrect_states(self):
        for incorrect_state in ["idle", "running", "some_arbitrary_state"]:
            with self.subTest(incorrect_state):
                with self.assertRaises(ValueError):
                    self.get_table(state=incorrect_state)

    @patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
    @patch(
        "neptune.internal.backends.neptune_backend_mock.NeptuneBackendMock.search_leaderboard_entries",
        new=lambda *args, **kwargs: [
            LeaderboardEntry(
                id="123",
                attributes=[
                    AttributeWithProperties(
                        "sys/creation_time",
                        AttributeType.DATETIME,
                        {"value": datetime(2024, 2, 5, 20, 37, 40, 915000).strftime(DATE_FORMAT)},
                    )
                ],
            )
        ],
    )
    def test_creation_time_returned_as_datetime(self):
        table = self.get_table()
        val = table.to_rows()[0].get_attribute_value("sys/creation_time")
        assert val == datetime(2024, 2, 5, 20, 37, 40, 915000)
