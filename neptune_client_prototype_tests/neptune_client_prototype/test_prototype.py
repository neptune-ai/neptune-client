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

from neptune_client_prototype.experiment import Experiment

# pylint: disable=protected-access


class TestExperiment(unittest.TestCase):

    def test_atom_ops(self):
        # given
        e = Experiment()
        # when
        e['atom'].assign(1)
        # then
        assert e['atom'].read() == 1
        # assert ops[-1] == (['atom'], 'assign', 1)

    def test_atom_reassignment(self):
        # given
        e = Experiment()
        # when
        e['atom'].assign(1)
        e['atom'].assign(2)
        # then
        assert e['atom'].read() == 2
        # assert ops[-2] == (['atom'], 'assign', 1)
        # assert ops[-1] == (['atom'], 'assign', 2)

    def test_set_ops(self):
        # given
        e = Experiment()
        # when
        e['set'].add(1, 2)
        # then
        assert e['set'].get() == {1, 2}
        # assert ops[-1] == (['set'], 'add', (1, 2))

    def test_set_reset(self):
        # given
        e = Experiment()
        e['set'].add(1, 2)
        # when
        e['set'].reset(3, 4)
        # then
        assert e['set'].get() == {3, 4}
        # assert ops[-1] == (['set'], 'reset', (3, 4))

    def test_set_remove(self):
        # given
        e = Experiment()
        e['set'].add(1, 2, 3)
        # when
        e['set'].remove(1, 3)
        # then
        assert e['set'].get() == {2}
        # assert ops[-1] == (['set'], 'remove', (1, 3))

    def test_set_batch_update(self):
        e = Experiment()
        e['foo'].add({'bar': 42, 'baz': (43, 44)})
        assert e['foo/bar'].get() == {42}
        assert e['foo/baz'].get() == {43, 44}

    class Wildcard():

        def __eq__(self, _):
            return True

    def test_series_log(self):
        # given
        e = Experiment()
        # when
        e['series'].log(42)
        e['series'].log(84)
        e['series'].log(168)
        # then
        # assert e['series'].tail(2) == [84, 168]
        # assert ops[-3] == (['series'], 'log', (0, Wildcard(), 42))
        # assert ops[-2] == (['series'], 'log', (1, Wildcard(), 84))
        # assert ops[-1] == (['series'], 'log', (2, Wildcard(), 168))

    # def test_series_batch_update(self):
    #    e = Experiment()
    #    e['foo'].log({'bar': 1, 'baz': 2})
    #    e['foo'].log({'bar': 2, 'baz': 3, 'xyz': 0})
    #    assert e['foo/bar'].tail(2) == [1, 2]
    #    assert e['foo/baz'].tail(2) == [2, 3]
    #    assert e['foo/xyz'].tail(1) == [0]

    def test_update_wrong_structure(self):
        # TODO add meaningful error messages
        e = Experiment()
        e['foo'].assign(1)
        with self.assertRaises(AttributeError):
            e['foo'].add('bar')

    def test_attempt_namespace_update(self):
        e = Experiment()
        e['foo/bar'].assign(1)
        with self.assertRaises(AttributeError) as excinfo:
            e['foo'].assign(2)
        assert 'cannot assign to an existing namespace' in str(excinfo.exception)

    def test_getter_of_nonexistent_variable(self):
        e = Experiment()
        with self.assertRaises(AttributeError):
            e['foo'].tail(2)

    def test_change_atom_type(self):
        e = Experiment()
        e['foo'].assign(1)
        e['foo'].assign('bar')
        assert e['foo'].read() == 'bar'
        assert e._members['foo']._type == str

    def test_attempt_change_series_type(self):
        e = Experiment()
        e['foo'].log('bar')
        with self.assertRaises(TypeError) as excinfo:
            e['foo'].log(42)
        assert 'cannot log a new type to a series' in str(excinfo.exception)
