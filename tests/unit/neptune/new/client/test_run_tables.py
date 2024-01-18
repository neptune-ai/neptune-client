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
from typing import List

from mock import patch

from neptune import init_project
from neptune.exceptions import NeptuneException
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.internal.container_type import ContainerType
from neptune.metadata_containers.metadata_containers_table import (
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
