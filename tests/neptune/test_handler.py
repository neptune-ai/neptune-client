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

# pylint: disable=protected-access
from neptune.internal.sync_neptune_backend_mock import SyncNeptuneBackendMock
from neptune.variable import FloatVariable, StringVariable, FloatSeriesVariable, StringSeriesVariable, StringSetVariable


class TestHandler(unittest.TestCase):

    def test_set(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/num/val'] = 5
        exp['some/str/val'] = "some text"
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringVariable)

    def test_assign(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/num/val'].assign(5)
        exp['some/str/val'].assign("some text")
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringVariable)

    def test_log(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/num/val'].log(5)
        exp['some/str/val'].log("some text")
        # TODO: self.assertEqual(exp['some/num/val'].get_values(), 5)
        # TODO: self.assertEqual(exp['some/str/val'].get_values(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatSeriesVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSeriesVariable)

    def test_insert(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/str/val'].insert("some text", "something else")
        self.assertEqual(exp['some/str/val'].get(), {"some text", "something else"})
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSetVariable)

    def test_pop(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/num/val'].assign(3)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        ns.pop('num/val')
        self.assertNotIn('some', exp.get_structure())

    def test_del(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        exp['some/num/val'].assign(3)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        del ns['num/val']
        self.assertNotIn('some', exp.get_structure())

    def test_lookup(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        ns = exp['some/ns']
        ns['val'] = 5
        self.assertEqual(exp['some/ns/val'].get(), 5)

        ns = exp['other/ns']
        exp['other/ns/some/value'] = 3
        self.assertEqual(ns['some/value'].get(), 3)

    def test_attribute_error(self):
        server = SyncNeptuneBackendMock()
        exp = server.create_experiment()
        with self.assertRaises(AttributeError):
            exp['var'].something()
