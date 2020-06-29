from .experiment import Experiment, ExperimentView

import time

def test_getitem():
    e = Experiment()
    ev = e['foo']
    ev1 = ev['bar']
    assert ev1._experiment is e
    assert ev1._path == ['foo', 'bar']

def test_assign_new():
    e = Experiment()
    e['foo'].assign(1)
    assert e._members['foo']._value == 1

def test_assign_existing():
    e = Experiment()
    e['foo'].assign(1)
    e['foo'].assign(2)
    assert e._members['foo']._value == 2

def test_series_log_new():
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

def test_set_add_new():
    e = Experiment()
    e['foo'].add('tag1', 'tag2')
    assert e['foo'].get() == {'tag2', 'tag1'}