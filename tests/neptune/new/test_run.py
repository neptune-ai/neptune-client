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
from datetime import datetime

from neptune.new import ANONYMOUS, init
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import MetadataInconsistency, InactiveRunException
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.series import FloatSeries, StringSeries


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

    def test_pop_namespace(self):
        exp = init(mode="debug", flush_period=0.5)
        exp.define("some/path/subpath/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        exp.define("some/otherpath", Float(4))
        exp.pop("some/path")
        self.assertTrue('path' not in exp.get_structure()['some'])

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
        now = datetime.now()
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
            },
            "simple_types": {
                "int": 42,
                "str": "imagine",
                "float": 3.14,
                "datetime": now,
                "list": list(range(10)),
            }
        })
        self.assertEqual(exp['x'].fetch(), 5)
        self.assertEqual(exp['metadata/name'].fetch(), "Trol")
        self.assertEqual(exp['metadata/age'].fetch(), 376)
        self.assertEqual(exp['toys'].fetch_last(), "hat")
        self.assertEqual(exp['nested/nested/deep_secret'].fetch_last(), 15)
        self.assertEqual(exp['simple_types/int'].fetch(), 42)
        self.assertEqual(exp['simple_types/str'].fetch(), 'imagine')
        self.assertEqual(exp['simple_types/float'].fetch(), 3.14)
        self.assertEqual(exp['simple_types/datetime'].fetch(),
                         now.replace(microsecond=1000 * int(now.microsecond / 1000)))
        self.assertEqual(exp['simple_types/list'].fetch(), str(list(range(10))))

    def test_assign_false(self):
        # https://github.com/neptune-ai/neptune-client/issues/555
        exp = init(mode="debug")
        exp["params"] = {'predictor.cheat': False}

        self.assertFalse(exp["params/predictor.cheat"].fetch())

    def test_access_blocked_after_stop(self):
        exp = init(mode="debug")
        exp['attr1'] = 1

        exp.stop()

        with self.assertRaises(InactiveRunException):
            exp['attr1'].fetch()
        with self.assertRaises(InactiveRunException):
            exp['attr2'] = 2
        with self.assertRaises(InactiveRunException):
            exp['series'].log(1)
