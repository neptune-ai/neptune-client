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
import time

from neptune_client_prototype.experiment import Experiment

# pylint: disable=protected-access


class TestExperiment(unittest.TestCase):

    def test_getitem(self):
        e = Experiment()
        ev = e['foo']
        ev1 = ev['bar']
        assert ev1._experiment is e
        assert ev1._path == ['foo', 'bar']

    def test_assign_new(self):
        e = Experiment()
        e['foo'].assign(1)
        assert e._members['foo']._value == 1

    def test_assign_existing(self):
        e = Experiment()
        e['foo'].assign(1)
        e['foo'].assign(2)
        assert e._members['foo']._value == 2

    def test_series_log_new(self):
        e = Experiment()
        e['foo'].log(42)
        e['foo'].log(84)
        step0, timestamp0, entry0 = e._members['foo']._values[0]
        step1, timestamp1, entry1 = e._members['foo']._values[1]
        now = time.time()
        assert step0 == 0
        assert now - 1 < timestamp0 < now
        assert entry0 == 42
        assert step1 == 1
        assert now - 1 < timestamp1 < now
        assert entry1 == 84

    def test_set_add_new(self):
        e = Experiment()
        e['foo'].add('tag1', 'tag2')
        assert e['foo'].get() == {'tag2', 'tag1'}
