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

from neptune.management.internal.utils import normalize_project_name
from neptune.management.exceptions import (
    InvalidProjectName,
    ConflictingWorkspaceName,
    MissingWorkspaceName,
)


class TestManagementUtils(unittest.TestCase):
    def test_normalize_project_name(self):
        self.assertEqual(
            "jackie/sandbox", normalize_project_name(name="jackie/sandbox")
        )
        self.assertEqual(
            "jackie/sandbox", normalize_project_name(name="sandbox", workspace="jackie")
        )
        self.assertEqual(
            "jackie/sandbox",
            normalize_project_name(name="jackie/sandbox", workspace="jackie"),
        )

        with self.assertRaises(InvalidProjectName):
            normalize_project_name(name="nothing/else/matters")

        with self.assertRaises(MissingWorkspaceName):
            normalize_project_name(name="sandbox")

        with self.assertRaises(ConflictingWorkspaceName):
            normalize_project_name(name="jackie/sandbox", workspace="john")
