#####################
### Neptune types
#####################

class Typ:
    pass

###################################
### Mock for testing
###################################

last_op = [None]

###################################
### Variables and structures
###################################

class Variable:
  
    def __init__(self, typ):
        super().__init__()
        # TODO validate type
        self.typ = typ

    # Structures Atom/Series/Set should override their respective method
    # assign/log/set when inheriting from Variable.

    def _wrong_structure_method_attempted(self):
        raise TypeError() # TODO message

    def assign(self, *args, **kwargs):
        self._wrong_structure_method_attempted()

    def log(self, *args, **kwargs):
        self._wrong_structure_method_attempted()

    def add(self, *args, **kwargs):
        self._wrong_structure_method_attempted()

class Atom(Variable):
    """
    typ: supported Python type or Neptune type
    """

    def assign(self, value):
        last_op[0] = ('atom.assign', convert_type(self.__class__, value))

class Series(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, typ):
        # TODO check that the type is supported by the Series structure 
        super().__init__(typ)

    def log(self, value):
        last_op[0] = ('series.log', convert_type(self.__class__, value))

class Set(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, typ):
        # TODO check that the type is supported by the Set structure 
        super().__init__(typ)

    def add(self, value):
        last_op[0] = ('set.add', convert_type(self.__class__, value))

###################################
### Implicit mappings
###################################

# FIXME We provide a global list of conversions in the prototype.
# Ideally, there should be a way to register custom conversions.
# Also, lookup should be made efficient.

# The format is (structure_type, external_type, neptune_conversion)
implicit_conversions = [
    (Atom, int, int),
    (Series, int, float)
]

def convert_type(structure_type, value):
    for (st, et, conversion) in implicit_conversions:
        if issubclass(structure_type, st) and isinstance(value, et):
            return conversion(value)
    raise ValueError()

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

class Experiment:
  
    def __init__(self):
        super().__init__()
        self._members = {}

    def _get_variable(self, path):
        """
        path: Path
        """
        return self._members.get(path)

    def __getitem__(self, path):
        """
        path: string
        """
        return ExperimentView(self, Path(path))

    def assign(self, path, value):
        pass

    def log(self, path, value, step, timestamp):
        pass

    def add(self, path, *values):
        pass

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

    def assign(self, value):
        self._experiment.assign(self._path, value)

    def log(self, value, step=None, timestamp=None):
        self._experiment.log(self._path, value, step, timestamp)

    def add(self, *values):
        self._experiment.add(self._path, *values)
