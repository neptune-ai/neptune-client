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
import pytest

from neptune.management.exceptions import (
    ConflictingWorkspaceName,
    InvalidProjectName,
    MissingWorkspaceName,
)
from neptune.management.internal.utils import normalize_project_name, ProjectKeyGenerator


class TestManagementUtils(unittest.TestCase):
    def test_normalize_project_name(self):
        self.assertEqual("jackie/sandbox", normalize_project_name(name="jackie/sandbox"))
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


@pytest.mark.parametrize("test_input,expected", [
    (("ddd", {"aaa", "bbb", "ccc"}), "ddd"),
    (("aaaa", {"aaaa", "bbb", "ccc"}), "aaa"),
    (("aaaa", {"aaa", "bbb", "ccc"}), "aaaa"),
    (("aaa3", {"aaa", "aaa2" "bbb", "ccc"}), "aaa3"),
    (("aaaa", {"aaa", "aaa2" "bbb", "ccc"}), "aaaa"),
    (("aaaa", {"aaa", "aaa2", "aaaa", "bbb", "ccc"}), "aaaa2"),
    (("aaaa", {"aaa", "aaa2", "aaaa", "aaaa2", "bbb", "ccc"}), "aaa3"),
])
def test_project_key_simple_generation(test_input, expected):
    result = ProjectKeyGenerator(test_input[0], test_input[1]).get_default_project_key()
    assert expected == result


@pytest.mark.parametrize("test_input", [
    ("ccc", {"aaa", "bbb", "ccc", "ccc2", "ccc3"}),
    ("aaaa", {"aaa", "aaa2", "aaa3", "aaaa", "aaaa2", "aaaa3", "bbb", "ccc"}),
])
def test_project_key_with_random_generation(test_input):
    project_name = test_input[0]
    existing_project_keys = test_input[1]
    result = ProjectKeyGenerator(project_name, existing_project_keys).get_default_project_key()
    assert len(result) == len(project_name) + 3
    assert result[:-ProjectKeyGenerator._PROJECT_KEY_RANDOM_SUFFIX_SIZE] == project_name
