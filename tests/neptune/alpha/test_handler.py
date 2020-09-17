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
import os
import unittest

# pylint: disable=protected-access

from neptune.alpha import init, ANONYMOUS
from neptune.alpha.envs import PROJECT_ENV_NAME, API_TOKEN_ENV_NAME
from neptune.alpha.types.atoms.float import Float as FloatVal
from neptune.alpha.types.atoms.string import String as StringVal
from neptune.alpha.types.series.float_series import FloatSeries as FloatSeriesVal
from neptune.alpha.types.series.string_series import StringSeries as StringSeriesVal
from neptune.alpha.types.sets.string_set import StringSet as StringSetVal
from neptune.alpha.attributes.atoms.float import Float
from neptune.alpha.attributes.atoms.string import String
from neptune.alpha.attributes.atoms.file import File
from neptune.alpha.attributes.series.float_series import FloatSeries
from neptune.alpha.attributes.series.string_series import StringSeries
from neptune.alpha.attributes.sets.string_set import StringSet


class TestHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_set(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'] = 5
        exp['some/str/val'] = "some text"
        exp.wait()
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], Float)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], String)

    def test_assign_atom(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'].assign(5)
        exp['some/str/val'].assign("some text", wait=True)
        self.assertEqual(exp['some/num/val'].get(), 5)
        self.assertEqual(exp['some/str/val'].get(), "some text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], Float)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], String)

        exp['some/num/val'].assign(FloatVal(15))
        exp['some/str/val'].assign(StringVal("other text"), wait=True)
        self.assertEqual(exp['some/num/val'].get(), 15)
        self.assertEqual(exp['some/str/val'].get(), "other text")
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], Float)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], String)

    def test_save_file(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/file'].save("path/to/other/file.txt")
        # TODO: Test download
        self.assertIsInstance(exp.get_structure()['some']['num']['file'], File)

    def test_assign_series(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'].assign(FloatSeriesVal([1, 2, 0, 10]))
        exp['some/str/val'].assign(StringSeriesVal(["text1", "text2"]), wait=True)
        # TODO: Assert fetching value
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatSeries)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSeries)

        exp['some/num/val'].assign(FloatSeriesVal([122, 543, 2, 5]))
        exp['some/str/val'].assign(StringSeriesVal(["other 1", "other 2", "other 3"]), wait=True)
        # TODO: Assert fetching value
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatSeries)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSeries)

    def test_log(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'].log(5)
        exp['some/str/val'].log("some text")
        # TODO: Assert fetching value
        self.assertIsInstance(exp.get_structure()['some']['num']['val'], FloatSeries)
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSeries)

    def test_assign_set(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/str/val'].assign(StringSetVal(["tag1", "tag2"]), wait=True)
        self.assertEqual(exp['some/str/val'].get(), {"tag1", "tag2"})
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSet)

        exp['some/str/val'].assign(StringSetVal(["other_1", "other_2", "other_3"]), wait=True)
        self.assertEqual(exp['some/str/val'].get(), {"other_1", "other_2", "other_3"})
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSet)

    def test_add(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/str/val'].add("some text", "something else", wait=True)
        self.assertEqual(exp['some/str/val'].get(), {"some text", "something else"})
        self.assertIsInstance(exp.get_structure()['some']['str']['val'], StringSet)

    def test_pop(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'].assign(3, wait=True)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        ns.pop('num/val', wait=True)
        self.assertNotIn('some', exp.get_structure())

    def test_del(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        exp['some/num/val'].assign(3)
        self.assertIn('some', exp.get_structure())
        ns = exp['some']
        del ns['num/val']
        self.assertNotIn('some', exp.get_structure())

    def test_lookup(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        ns = exp['some/ns']
        ns['val'] = 5
        exp.wait()
        self.assertEqual(exp['some/ns/val'].get(), 5)

        ns = exp['other/ns']
        exp['other/ns/some/value'] = 3
        exp.wait()
        self.assertEqual(ns['some/value'].get(), 3)

    def test_attribute_error(self):
        exp = init(connection_mode="offline", flush_period=0.5)
        with self.assertRaises(AttributeError):
            exp['var'].something()
