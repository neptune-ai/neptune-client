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
import uuid

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.run_structure import ContainerStructure
from neptune.new.types.value import Value


class TestRunStructure(unittest.TestCase):
    def test_get_none(self):
        exp = ContainerStructure[int, dict]()
        self.assertEqual(exp.get(["some", "path", "val"]), None)

    def test_get_nested_variable_fails(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.get(["some", "path", "val", "nested"])
        with self.assertRaises(MetadataInconsistency):
            exp.get(["some", "path", "val", "nested", "nested"])

    def test_get_ns(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        self.assertEqual(exp.get(["some", "path"]), {"val": 3})

    def test_set(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        self.assertEqual(exp.get(["some", "path", "val"]), 3)

    def test_set_nested_variable_fails(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path", "val", "nested"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path", "val", "nested", "nested"], 3)

    def test_set_ns_collision(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.set(["some", "path"], 5)

    def test_pop(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val1"], 3)
        exp.set(["some", "path", "val2"], 5)
        exp.pop(["some", "path", "val2"])
        self.assertEqual(exp.get(["some", "path", "val1"]), 3)
        self.assertEqual(exp.get(["some", "path", "val2"]), None)
        self.assertTrue(
            "some" in exp.get_structure() and "path" in exp.get_structure()["some"]
        )

    def test_pop_whole_ns(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val"], 3)
        exp.pop(["some", "path", "val"])
        self.assertEqual(exp.get(["some", "path", "val"]), None)
        self.assertFalse("some" in exp.get_structure())

    def test_pop_not_found(self):
        exp = ContainerStructure[int, dict]()
        with self.assertRaises(MetadataInconsistency):
            exp.pop(["some", "path"])

    def test_pop_ns_fail(self):
        exp = ContainerStructure[int, dict]()
        exp.set(["some", "path", "val1"], 3)
        with self.assertRaises(MetadataInconsistency):
            exp.pop(["some", "path"])


class TestIterateSubpaths(unittest.TestCase):
    # pylint: disable=protected-access
    project_uuid = str(uuid.uuid4())

    def setUp(self):
        self.backend = NeptuneBackendMock()
        exp = self.backend.create_run(self.project_uuid)
        # FIXME test for projects
        self.structure = self.backend._containers[(exp.id, ContainerType.RUN)]
        self.structure.set(["attributes", "float"], Value())
        self.structure.set(["attributes", "node", "one"], Value())
        self.structure.set(["attributes", "node", "two"], Value())
        self.structure.set(["attributes", "node", "three"], Value())
        self.structure.set(["attributes", "int"], Value())
        self.structure.set(["attributes", "string"], Value())

    def test_iterate_empty_run(self):
        empty_structure = ContainerStructure[Value, dict]()

        self.assertListEqual(list(empty_structure.iterate_subpaths([])), [])
        self.assertListEqual(list(empty_structure.iterate_subpaths(["test"])), [])

    def test_iterate_empty_prefix(self):
        prefix = []
        expected_subpaths = [
            "sys/id",
            "sys/state",
            "sys/owner",
            "sys/size",
            "sys/tags",
            "sys/creation_time",
            "sys/modification_time",
            "sys/failed",
            "attributes/float",
            "attributes/int",
            "attributes/string",
            "attributes/node/one",
            "attributes/node/two",
            "attributes/node/three",
        ]

        self.assertListEqual(
            list(self.structure.iterate_subpaths(prefix)), expected_subpaths
        )

    def test_iterate_prefix(self):
        prefix = ["sys"]
        expected_subpaths = [
            "sys/id",
            "sys/state",
            "sys/owner",
            "sys/size",
            "sys/tags",
            "sys/creation_time",
            "sys/modification_time",
            "sys/failed",
        ]

        self.assertListEqual(
            list(self.structure.iterate_subpaths(prefix)), expected_subpaths
        )

    def test_iterate_long_prefix(self):
        prefix = ["attributes", "node"]
        expected_subpaths = [
            "attributes/node/one",
            "attributes/node/two",
            "attributes/node/three",
        ]

        self.assertListEqual(
            list(self.structure.iterate_subpaths(prefix)), expected_subpaths
        )

    def test_iterate_nonexistent_prefix(self):
        prefix = ["argh"]
        expected_subpaths = []

        self.assertListEqual(
            list(self.structure.iterate_subpaths(prefix)), expected_subpaths
        )
