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

from neptune import init
from neptune.variable import FloatVariable, StringVariable, FloatSeriesVariable, StringSeriesVariable, StringSetVariable


class TestHandler(unittest.TestCase):

    def test_set(self):
        exp = init(flush_period=0.5)
        exp['some/num/val'] = 5
        exp['some/str/val'] = "some text"
        exp.wait()
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringVariable)

    def test_assign(self):
        exp = init(flush_period=0.5)
        exp['some/num/val'].assign(5)
        exp['some/str/val'].assign("some text", wait=True)
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringVariable)

    def test_log(self):
        exp = init(flush_period=0.5)
        exp['some/num/val'].log(5)
        exp['some/str/val'].log("some text")
        # TODO: self.assertEqual(exp['some/num/val'].get_values(), 5)
        # TODO: self.assertEqual(exp['some/str/val'].get_values(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatSeriesVariable)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSeriesVariable)

    def test_insert(self):
        exp = init(flush_period=0.5)
        exp['some/str/val'].insert("some text", "something else", wait=True)
        self.assertEqual(exp['some/str/val'].get(), {"some text", "something else"})
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSetVariable)

    def test_pop(self):
        exp = init(flush_period=0.5)
        exp['some/num/val'].assign(3, wait=True)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        ns.pop('num/val', wait=True)
        self.assertNotIn('some', exp.get_structure())

    def test_del(self):
        exp = init(flush_period=0.5)
        exp['some/num/val'].assign(3)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        del ns['num/val']
        self.assertNotIn('some', exp.get_structure())

    def test_lookup(self):
        exp = init(flush_period=0.5)
        ns = exp['some/ns']
        ns['val'] = 5
        exp.wait()
        self.assertEqual(exp['some/ns/val'].get(), 5)

        ns = exp['other/ns']
        exp['other/ns/some/value'] = 3
        exp.wait()
        self.assertEqual(ns['some/value'].get(), 3)

    def test_attribute_error(self):
        exp = init(flush_period=0.5)
        with self.assertRaises(AttributeError):
            exp['var'].something()
