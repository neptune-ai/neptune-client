from .experiment import _Path

import pytest

def test_init_from_string():
    p = _Path('foo/bar')
    assert p._value == ['foo', 'bar']

def test_init_from_list():
    list_ = ['foo', 'bar']
    p = _Path(list_)
    assert p._value == ['foo', 'bar']
    list_[1] = 'baz'
    assert p._value == ['foo', 'bar']

def test_segment_lookup():
    p = _Path('foo/bar')
    assert p[-1] == 'bar'

def test_subpath_lookup():
    p = _Path('foo/bar/baz')
    assert p[:-1] == _Path('foo/bar')

def test_path_iterator():
    p = _Path('foo/bar')
    i = iter(p)
    assert next(i) == 'foo'
    assert next(i) == 'bar'
    with pytest.raises(StopIteration):
        next(i)

def test_path_addition