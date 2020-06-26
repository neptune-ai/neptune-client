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
        self._values = {}

    # TODO mark interface methods with decorators?
    interface = ['get', 'set', 'insert', 'remove']

    def get(self):
        return self._values

    def set(self, *values):
        self._values = set(values)

    def add(self, *values):
        ops.append((self._path, 'add', values))
        self._values += values

    def remove(self, *values):
        self._values -= values

Variable.structures = [Atom, Series, Set]
