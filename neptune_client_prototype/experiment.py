from .variable import *

# pylint: disable=protected-access

class _Path:

    def __init__(self, path):
        """
        path: str
        """
        super().__init__()
        # TODO validate and normalize to /path/to/variable
        self._value = path

    def __add__(self, other):
        """
        other: string or _Path
        """
        if isinstance(other, str):
            return self + _Path(other)
        elif isinstance(other, _Path):
            return _Path(self._value + other._value)
        else:
            raise ValueError()

    def __eq__(self, other):
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

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
        return self._members.get(path)

    def _set_variable(self, path, variable):
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
        self.add(*values)

    def __getattr__(self, attr):
        pass
