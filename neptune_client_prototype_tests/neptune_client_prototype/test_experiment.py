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

from neptune_client_prototype.experiment import Experiment, Handler, parse_path

# pylint: disable=protected-access


class TestExperiment(unittest.TestCase):

    def test_get_variable(self):
        # given
        e = Experiment()
        e._members = {
            'foo': {
                'bar': 1
            }
        }
        # when
        result = e._get_variable(['foo', 'bar'])
        # then
        assert result == 1

    def test_set_variable(self):
        e = Experiment()
        e._set_variable(['foo', 'bar'], 2)
        assert e._members['foo']['bar'] == 2

    def test_set_variable_2(self):
        e = Experiment()
        e._members = {
            'foo': {}
        }
        e._set_variable(['foo', 'bar'], 2)
        assert e._members['foo']['bar'] == 2

    def test_parse_path(self):
        assert parse_path('foo/bar') == ['foo', 'bar']

    def test_experiment_getitem(self):
        e = Experiment()
        ev = e['foo/bar']
        assert isinstance(ev, Handler)
        assert ev._experiment is e
        assert ev._path == ['foo', 'bar']
