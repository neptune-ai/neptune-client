from .experiment import Experiment
from .variable import ops

import pytest

def test_atom_ops():
    # given
    e = Experiment()
    # when
    e['atom'].assign(1)
    # then
    assert e['atom'].read() == 1
    assert ops[-1] == (['atom'], 'assign', 1)

def test_atom_reassignment():
    # given
    e = Experiment()
    # when
    e['atom'].assign(1)
    e['atom'].assign(2)
    # then
    assert e['atom'].read() == 2
    assert ops[-2] == (['atom'], 'assign', 1)
    assert ops[-1] == (['atom'], 'assign', 2)

def test_batch_assign():
    e = Experiment()
    e['foo'].assign({'bar': 1, 'baz': 2})
    assert e['foo/bar'].read == 1
    assert e['foo/baz'].read == 2

def test_set_ops():
    # given
    e = Experiment()
    # when
    e['set'].add(1, 2)
    # then
    assert e['set'].get() == {1, 2}
    assert ops[-1] == (['set'], 'add', (1, 2))

def test_set_reset():
    # given
    e = Experiment()
    e['set'].add(1, 2)
    # when
    e['set'].reset(3, 4)
    # then
    assert e['set'].get() == {3, 4}
    assert ops[-1] == (['set'], 'reset', (3, 4))

def test_set_remove():
    # given
    e = Experiment()
    e['set'].add(1, 2, 3)
    # when
    e['set'].remove(1, 3)
    # then
    assert e['set'].get() == {2}
    assert ops[-1] == (['set'], 'remove', (1, 3))

def test_set_batch_update():
    e = Experiment()
    e['foo'].add({'bar': 42, 'baz': (43, 44)})
    assert e['foo/bar'].get() == {42}
    assert e['foo/baz'].get() == {43, 44}

class Wildcard():

    def __eq__(self, _):
        return True

def test_series_log():
    # given
    e = Experiment()
    # when
    e['series'].log(42)
    e['series'].log(84)
    e['series'].log(168)
    # then
    assert e['series'].tail(2) == [84, 168]
    assert ops[-3] == (['series'], 'log', (0, Wildcard(), 42))
    assert ops[-2] == (['series'], 'log', (1, Wildcard(), 84))
    assert ops[-1] == (['series'], 'log', (2, Wildcard(), 168))

def test_series_batch_update():
    e = Experiment()
    e['foo'].log({'bar': 1, 'baz': 2})
    e['foo'].log({'bar': 2, 'baz': 3, 'xyz': 0})
    assert e['foo/bar'].tail(2) == [1, 2]
    assert e['foo/baz'].tail(2) == [2, 3]
    assert e['foo/xyz'].tail(1) == [0]

def test_update_wrong_structure():
    # TODO add meaningful error messages
    e = Experiment()
    e['foo'].assign(1)
    with pytest.raises(AttributeError):
        e['foo'].add('bar')

def test_attempt_namespace_update():
    e = Experiment()
    e['foo/bar'].assign(1)
    with pytest.raises(AttributeError) as excinfo:
        e['foo'].assign(2)
    assert 'cannot assign to an existing namespace' in str(excinfo.value)

def test_getter_of_nonexistent_variable():
    e = Experiment()
    with pytest.raises(AttributeError):
        e['foo'].tail(2)

def test_change_atom_type():
    e = Experiment()
    e['foo'].assign(1)
    e['foo'].assign('bar')
    assert e['foo'].read() == 'bar'
    assert e._members['foo']._type == str

def test_attempt_change_series_type():
    e = Experiment()
    e['foo'].log('bar')
    with pytest.raises(TypeError) as excinfo:
        e['foo'].log(42)
    assert 'cannot log a new type to a series' in str(excinfo.value)