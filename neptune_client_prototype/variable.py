# pylint: disable=protected-access

import time
from datetime import datetime

#####################
### Neptune types
#####################

class Typ:
    pass

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
        self._metadata = {}

    def add_metadata(self, key, value):
        self._metadata[key] = value

class Atom(Variable):
    """
    typ: supported Python type or Neptune type
    """

    def __init__(self, experiment, path, value):
        typ, value = self._convert_type(value)
        super().__init__(experiment, path, typ)
        self.assign(value)

    def _convert_type(self, value):
        # TODO support all supported types
        supported_types = [int, float, str, datetime]
        for typ in supported_types:
            if isinstance(value, typ):
                return typ, value
        raise TypeError('type not supported')

    def read(self):
        return self._value

    # TODO allow for changing type?
    def assign(self, value):
        typ, value = self._convert_type(value)
        ops.append((self._path, 'assign', value))
        self._value = value
        self._type = typ

class Series(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, experiment, path):
        # TODO check that the type is supported by the Series structure
        super().__init__(experiment, path, None)
        self._values = []

    def _convert_type(self, value):
        # TODO support all supported types
        supported_types = [str, datetime]
        for typ in supported_types:
            if isinstance(value, typ):
                return typ, value
        if isinstance(value, int) or isinstance(value, float):
            return float, float(value)
        raise TypeError('type not supported')

    def _next_step(self):
        if self._values:
            last_step = self._values[-1][0]
            return last_step + 1
        else:
            return 0

    # TODO is a picosecond good enough? I am not aware of systems which track 
    # the current unixtime down to picoseconds
    tiebreaker_nanosecond = 1e-12

    def _next_timestamp(self):
        if self._values:
            last_timestamp = self._values[-1][1]
            now = time.time()
            if now == last_timestamp:
                return now + self.tiebreaker_nanosecond
            else:
                return now
        else:
            return time.time()

    def log(self, value, step=None, timestamp=None):
        # TODO handle step and timestamp from user
        typ, value = self._convert_type(value)
        if self._type is None:
            self._type = typ
        elif self._type != typ:
            raise TypeError('cannot log a new type to a series')
        step = self._next_step()
        timestamp = self._next_timestamp()
        ops.append((self._path, 'log', (step, timestamp, value)))
        self._values.append((step, timestamp, value))

    def tail(self, n_last):
        return [v for _, _, v in self._values[-n_last:]]

class Set(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, experiment, path, typ):
        # TODO check that the type is supported by the Set structure
        super().__init__(experiment, path, typ)
        self._values = set()

    def get(self):
        return self._values

    def reset(self, *values):
        ops.append((self._path, 'reset', values))
        self._values = set(values)

    def add(self, *values):
        ops.append((self._path, 'add', values))
        self._values.update(values)

    def remove(self, *values):
        ops.append((self._path, 'remove', values))
        self._values.difference_update(values)
