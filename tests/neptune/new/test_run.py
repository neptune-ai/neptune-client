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

from neptune.new.types.series import StringSeries, FloatSeries

from neptune.new import init, ANONYMOUS
from neptune.new.envs import PROJECT_ENV_NAME, API_TOKEN_ENV_NAME
from neptune.new.exceptions import MetadataInconsistency
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String


class TestRun(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_define(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/value", Float(5), wait=True)
        self.assertEqual(exp.get_structure()['some']['path']['value'].fetch(), 5)

    def test_define_string(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/value", String("Some string"), wait=True)
        self.assertEqual(exp.get_structure()['some']['path']['value'].fetch(), "Some string")

    def test_define_few_variables(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"), wait=True)
        self.assertEqual(exp.get_structure()['some']['path']['num'].fetch(), 3)
        self.assertEqual(exp.get_structure()['some']['path']['text'].fetch(), "Some text")

    def test_define_conflict(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/value", Float(5))
        with self.assertRaises(MetadataInconsistency):
            exp.define("some/path/value", Float(1))

    def test_pop(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        exp.pop("some/path/text")
        self.assertTrue('num' in exp.get_structure()['some']['path'])
        self.assertTrue('text' not in exp.get_structure()['some']['path'])

    def test_run_as_handler(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        handler = exp['some/path']
        exp.wait()
        self.assertEqual(handler['num'].fetch(), 3)
        self.assertEqual(handler['text'].fetch(), "Some text")

    def test_assign_dict(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.assign({
            "x": 5,
            "metadata": {
                "name": "Trol",
                "age": 376
            },
            "toys": StringSeries(["cudgel", "hat"]),
            "nested": {
                "nested": {
                    "deep_secret": FloatSeries([13, 15])
                }
            }
        })
        self.assertEqual(exp['x'].fetch(), 5)
        self.assertEqual(exp['metadata/name'].fetch(), "Trol")
        self.assertEqual(exp['metadata/age'].fetch(), 376)
        self.assertEqual(exp['toys'].fetch_last(), "hat")
        self.assertEqual(exp['nested/nested/deep_secret'].fetch_last(), 15)
