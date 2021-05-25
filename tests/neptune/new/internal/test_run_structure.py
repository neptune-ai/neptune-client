#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.run_structure import RunStructure


class TestRunStructure(unittest.TestCase):

    def test_get_none(self):
        exp = RunStructure[int, dict]()
        self.assertEqual(exp.get(["some", "path", "val"]), None)

    def test_get_nested_variable_fails(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.get(["some", "path", "val", "nested"])
        with self.assertRaises(MetadataInconsistency):
            exp.get(["some", "path", "val", "nested", "nested"])

    def test_get_ns(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        self.assertEqual(exp.get(["some", "path"]), {"val": 3})

    def test_set(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        self.assertEqual(exp.get(["some", "path", "val"]), 3)

    def test_set_nested_variable_fails(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path", "val", "nested"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path", "val", "nested", "nested"], 3)

    def test_set_ns_collision(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path"], 5)

    def test_pop(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val1"], 3)
        exp.set(["some", "path", "val2"], 5)
        exp.pop(["some", "path", "val2"])
        self.assertEqual(exp.get(["some", "path", "val1"]), 3)
        self.assertEqual(exp.get(["some", "path", "val2"]), None)
        self.assertTrue("some" in exp.get_structure() and "path" in exp.get_structure()["some"])

    def test_pop_whole_ns(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        exp.pop(["some", "path", "val"])
        self.assertEqual(exp.get(["some", "path", "val"]), None)
        self.assertFalse("some" in exp.get_structure())

    def test_pop_not_found(self):
        exp = RunStructure[int, dict]()
        with self.assertRaises(MetadataInconsistency):
            exp.pop(["some", "path"])

    def test_pop_ns_fail(self):
        exp = RunStructure[int, dict]()
        exp.set(["some", "path", "val1"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.pop(["some", "path"])
