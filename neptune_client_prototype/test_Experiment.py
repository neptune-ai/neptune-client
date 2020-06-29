from .experiment import Experiment, ExperimentView, parse_path

def test_get_variable():
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

def test_set_variable():
    e = Experiment()
    e._set_variable(['foo', 'bar'], 2)
    assert e._members['foo']['bar'] == 2

def test_set_variable_2():
    e = Experiment()
    e._members = {
        'foo': {}
    }
    e._set_variable(['foo', 'bar'], 2)
    assert e._members['foo']['bar'] == 2

def test_parse_path():
    assert parse_path('foo/bar') == ['foo', 'bar']

def test_experiment_getitem():
    e = Experiment()
    ev = e['foo/bar']
    assert isinstance(ev, ExperimentView)
    assert ev._experiment is e
    assert ev._path == ['foo', 'bar']