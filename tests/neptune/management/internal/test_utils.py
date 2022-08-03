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
from neptune.management.internal.utils import (
    ProjectKeyGenerator,
    normalize_project_name,
)


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


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (("01a", {"AAA", "BBB", "CCC"}), "01A"),
        (("ddd", {"AAA", "BBB", "CCC"}), "DDD"),
        (("aaaa", {"AAAA", "BBB", "CCC"}), "AAA"),
        (("aa--aa", {"AAA", "BBB", "CCC"}), "AAAA"),
        (("aaaa", {"AAA", "BBB", "CCC"}), "AAAA"),
        (("aa---a", {"AAA", "BBB", "CCC"}), "AAA2"),
        (("a-a---a--a", {"AAAA", "BBB", "CCC"}), "AAA"),
        (("AAAA", {"AAA", "BBB", "CCC"}), "AAAA"),
        (("aaaa", {"aaa", "BBB", "CCC"}), "AAAA"),
        (("aaa3", {"AAA", "AAA2", "BBB", "CCC"}), "AAA3"),
        (("aaaa", {"AAA", "AAA2", "BBB", "CCC"}), "AAAA"),
        (("aaaa", {"AAA", "AAA2", "AAAA", "BBB", "CCC"}), "AAAA2"),
        (("aaaa", {"AAA", "AAA2", "AAAA", "AAAA2", "BBB", "CCC"}), "AAA3"),
        (("000-KEBAB", {"AAA", "BBB", "CCC"}), "000K"),
        (("000-KEBAB", {"AAA", "BBB", "CCC", "000K"}), "000KE"),
    ],
)
def test_project_key_simple_generation(test_input, expected):
    result = ProjectKeyGenerator(test_input[0], test_input[1]).get_default_project_key()
    assert expected == result


@pytest.mark.parametrize(
    "test_input,expected_prefix",
    [
        (("ccc", {"aaa", "bbb", "ccc", "ccc2", "ccc3"}), "CCC"),
        (("aaaa", {"aaa", "aaa2", "aaa3", "aaaa", "aaaa2", "aaaa3", "bbb", "ccc"}), "AAAA"),
        (("A----", {"AAA", "BBB", "CCC", "CCC2", "CCC3"}), "A"),
        (("----", {"AAA", "BBB", "CCC", "CCC2", "CCC3"}), ""),
        (("a-a", {"AAA", "BBB", "CCC", "CCC2", "CCC3"}), "AA"),
        (("A-A", {"AAA", "BBB", "CCC", "CCC2", "CCC3"}), "AA"),
        (("-0--1-----", {"AAA", "BBB", "CCC"}), "01"),
        (("-000--1--", {"AAA", "BBB", "CCC"}), "0001"),
        (("-000--111", {"AAA", "BBB", "CCC"}), "00011"),
        (("-0--!@#$%^&*()_+=-[]{};':,./<>?|--1-", {"AAA", "BBB", "CCC"}), "01"),  # not the case now
    ],
)
def test_project_key_with_random(test_input, expected_prefix):
    project_name = test_input[0]
    existing_project_keys = test_input[1]
    result = ProjectKeyGenerator(project_name, existing_project_keys).get_default_project_key()
    assert len(result) == len(expected_prefix) + 3
    assert result[:-3] == expected_prefix
