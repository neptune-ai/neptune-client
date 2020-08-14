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

from neptune.exceptions import MetadataInconsistency
from neptune.internal.sync_neptune_server_mock import SyncNeptuneServerMock
from neptune.types.atoms.float import Float

from neptune.types.atoms.string import String


class TestExperiment(unittest.TestCase):

    def test_define(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/value", Float(5))
        self.assertEqual(exp.get_structure()['some']['path']['value'].get(), 5)

    def test_define_string(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/value", String("Some string"))
        self.assertEqual(exp.get_structure()['some']['path']['value'].get(), "Some string")

    def test_define_few_variables(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        self.assertEqual(exp.get_structure()['some']['path']['num'].get(), 3)
        self.assertEqual(exp.get_structure()['some']['path']['text'].get(), "Some text")

    def test_define_conflict(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/value", Float(5))
        with self.assertRaises(MetadataInconsistency):
            exp.define("some/path/value", Float(1))

    def test_pop(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        exp.pop("some/path/text")
        self.assertTrue('num' in exp.get_structure()['some']['path'])
        self.assertTrue('text' not in exp.get_structure()['some']['path'])

    def test_experiment_as_handler(self):
        server = SyncNeptuneServerMock()
        exp = server.create_experiment()
        exp.define("some/path/num", Float(3))
        exp.define("some/path/text", String("Some text"))
        handler = exp['some/path']
        self.assertEqual(handler['num'].get(), 3)
        self.assertEqual(handler['text'].get(), "Some text")
