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

from neptune import init_model
from neptune.internal.container_type import ContainerType
from neptune.table import (
    Table,
    TableEntry,
)
from tests.unit.neptune.new.client.abstract_tables_test import AbstractTablesTestMixin


class TestModelVersionTables(AbstractTablesTestMixin, unittest.TestCase):
    expected_container_type = ContainerType.MODEL_VERSION

    def get_table(self, **kwargs) -> Table:
        return init_model(
            with_id="organization/project",
            project="PRO-MOD",
            mode="read-only",
        ).fetch_model_versions_table(**kwargs)

    def get_table_entries(self, table) -> List[TableEntry]:
        return table.to_rows()
