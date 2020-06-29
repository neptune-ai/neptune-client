# pylint: disable=protected-access

import time

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

class Atom(Variable):
    """
    typ: supported Python type or Neptune type
    """

    def __init__(self, experiment, path, value):
        super().__init__(experiment, path, type_placeholder)
        self.assign(value)

    def read(self):
        return self._value

    def assign(self, v):
        ops.append((self._path, 'assign', v))
        self._value = v

class Series(Variable):
    """
    typ: supported Python type or Neptune type
    """
    
    def __init__(self, experiment, path, typ):
        # TODO check that the type is supported by the Series structure
        super().__init__(experiment, path, typ)
        self._values = []

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
