from . import Experiment, ops, _Path

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