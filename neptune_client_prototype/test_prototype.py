from .experiment import Experiment
from .variable import ops

def test_atom_ops():
    # given
    e = Experiment()
    # when
    e['atom'].value = 1
    result = e['atom'].value
    # then
    assert result == 1
    assert ops[-1] == ('atom', 'value.setter', 1)

def test_atom_reassignment():
    # given
    e = Experiment()
    # when
    e['atom'].value = 1
    e['atom'].value = 2
    result = e['atom'].value
    # then
    assert result == 2
    assert ops[-2] == ('atom', 'value.setter', 1)
    assert ops[-1] == ('atom', 'value.setter', 2)

def test_set_ops():
    # given
    e = Experiment()
    # when
    e['set'].add(1, 2)
    result = e['set'].get()
    # then
    assert result == {1, 2}
    assert ops[-1] == ('set', 'add', (1, 2))

def test_set_reset():
    # given
    e = Experiment()
    # when
    e['set'].add(1, 2)
    e['set'].set(3, 4)
    result = e['set'].get()
    # then
    assert result == {3, 4}
    assert ops[-2] == ('set', 'add', (1, 2))
    assert ops[-1] == ('set', 'set', (3, 4))

def test_set_remove():
    # given
    e = Experiment()
    # when
    e['set'].add(1, 2, 3)
    e['set'].remove(1, 3)
    result = e['set'].get()
    # then
    assert result == {2}
    assert ops[-2] == ('set', 'add', (1, 2, 3))
    assert ops[-1] == ('set', 'remove', (1, 3))

def test_series_log():
    # given
    e = Experiment()
    # when
    e['series'].log(42, timestamp=1)
    result = e['series'].tail(1)
    # then
    assert result == [(1, 42)]
    assert ops[-2] == ('series', 'log', (None, 1, 42))
