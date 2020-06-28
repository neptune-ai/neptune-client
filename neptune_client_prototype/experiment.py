from .variable import *

from copy import copy

# pylint: disable=protected-access

class _Path:

    def __init__(self, path):
        """
        path: str
        """
        super().__init__()
        # TODO validate and normalize to /path/to/variable
        if isinstance(path, str):
            self._value = path.split('/')
        elif isinstance(path, list):
            self._value = copy(path)
        else:
            raise TypeError()

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._value[key]
        elif isinstance(key, slice):
            return _Path(self._value[key])
        else:
            raise TypeError()

    def __iter__(self):
        return iter(self._value)

    def __add__(self, other):
        """
        other: _Path
        """
        return _Path(self._value + other._value)

    def __eq__(self, other):
        return self._value == other._value

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"_Path({self._value})"

class Experiment:
  
    def __init__(self):
        super().__init__()
        self._members = {}

    def _get_variable(self, path):
        """
        path: _Path
        """
        # TODO handle non-existent path
        ref = self._members
        for segment in path:
            ref = ref[segment] 
        return ref

    def _set_variable(self, path, variable):
        namespace = self._members
        location, variable_name = path
        for segment in path:
            if segment in namespace:
                namespace = namespace[segment]
            else:
                namespace[segment] = {}
                namespace = namespace[segment]
        

        self._members[path] = variable

    def __getitem__(self, path):
        """
        path: string
        """
        return ExperimentView(self, _Path(path))


class ExperimentView:
  
    def __init__(self, experiment, path):
        """
        experiment: Experiment
        path: _Path
        """
        super().__init__()
        self._experiment = experiment
        self._path = path

    def __getitem__(self, path):
        """
        key: string
        """
        return ExperimentView(self._experiment, self._path + path)

    def _get_variable(self):
        return self._experiment._get_variable(self._path)

    def _set_variable(self, var):
        self._experiment._set_variable(self._path, var)

    @property
    def value(self):
        return self._get_variable().value

    @value.setter
    def value(self, v):
        var = self._get_variable()
        if var:
            var.value = v
        else:
            var = Atom(self._experiment, self._path, v)
            self._set_variable(var)

    def log(self, value, step=None, timestamp=None):
        var = self._experiment._get_variable(self._path)
        if not var:
            var = Series(self._experiment, self._path, type_placeholder)
            self._experiment._set_variable(self._path, var)
        var.log(value, step, timestamp)

    def add(self, *values):
        var = self._get_variable()
        if not var:
            var = Set(self._experiment, self._path, type_placeholder)
            self._set_variable(var)
        var.add(*values)

    def __getattr__(self, attr):
        var = self._get_variable()
        if var:
            return getattr(var, attr)
        else:
            return getattr(super(), attr)
