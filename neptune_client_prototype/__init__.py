# pylint: disable=protected-access

#####################
### Neptune types
#####################

class Typ:
    pass

type_placeholder = None

###################################
### Mock for testing
###################################

ops = []

###################################
### Variables and structures
###################################

class Variable:
    """
    """

    def __init__(self, experiment, path, typ):
        super().__init__()
        self._experiment = experiment
        self._path = path
        self._type = typ

    def str_path(self):
        return str(self._path)

class Atom(Variable):
    """
    typ: supported Python type or Neptune type
    """

    def __init__(self, experiment, path, value):
        super().__init__(experiment, path, type_placeholder)
        self.value = value

    @property
    def value(self):
        ops.append((self.str_path(), 'value.getter', self._value))
        return self._value

    @value.setter
    def value(self, v):
        ops.append((self.str_path(), 'value.setter', v))
        self._value = v

class Series(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, experiment, path, typ):
        # TODO check that the type is supported by the Series structure
        super().__init__(experiment, path, typ)

    interface = ['log', 'get', 'remove']

    def log(self, value, step=None, timestamp=None):
        pass

    def get(self, step=None, timestamp=None):
        pass

    def remove(self, step=None, timestamp=None):
        pass

class Set(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, experiment, path, typ):
        # TODO check that the type is supported by the Set structure
        super().__init__(experiment, path, typ)

    # TODO mark interface methods with decorators?
    interface = ['get', 'set', 'insert', 'remove']

    def get(self):
        pass

    def set(self, *values):
        pass

    def add(self, *values):
        pass

    def remove(self, *values):
        pass

Variable.structures = [Atom, Series, Set]

###################################
### Implicit mappings
###################################

# FIXME We provide a global list of conversions in the prototype.
# Ideally, there should be a way to register custom conversions.
# Also, lookup should be made efficient.

# # The format is (structure_type, external_type, neptune_conversion)
# implicit_conversions = [
#     (Atom, int, int),
#     (Series, int, float)
# ]

# def convert_type(structure_type, value):
#     for (st, et, conversion) in implicit_conversions:
#         if issubclass(structure_type, st) and isinstance(value, et):
#             return conversion(value)
#     raise ValueError()

###################################
### Namespace
###################################

# internal
class Path:

    def __init__(self, path):
        """
        path: str
        """
        super().__init__()
        # TODO validate and normalize to /path/to/variable
        self._value = path

    def __add__(self, other):
        """
        other: string or Path
        """
        if isinstance(other, str):
            return self + Path(other)
        elif isinstance(other, Path):
            return Path(self._value + other._value)
        else:
            raise ValueError()

    def __eq__(self, other):
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"Path({self._value})"

class Experiment:
  
    def __init__(self):
        super().__init__()
        self._members = {}

    def _get_variable(self, path):
        """
        path: Path
        """
        return self._members.get(path)

    def _set_variable(self, path, variable):
        self._members[path] = variable

    def __getitem__(self, path):
        """
        path: string
        """
        return ExperimentView(self, Path(path))

class ExperimentView:
  
    def __init__(self, experiment, path):
        """
        experiment: Experiment
        path: Path
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

    def assign(self, value):
        var = self._experiment._get_variable(self._path)
        if not var:
            var = Atom(self._experiment, self._path, type_placeholder)
            self._experiment._set_variable(self._path, var)
        var.assign(value)

    def log(self, value, step=None, timestamp=None):
        var = self._experiment._get_variable(self._path)
        if not var:
            var = Series(self._experiment, self._path, type_placeholder)
            self._experiment._set_variable(self._path, var)
        var.log(value, step, timestamp)

    def add(self, *values):
        var = self._experiment._get_variable(self._path)
        if not var:
            var = Set(self._experiment, self._path, type_placeholder)
            self._experiment._set_variable(self._path, var)
        var.add(*values)
