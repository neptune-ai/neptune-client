#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pylint: disable=protected-access

"""
Defines variables and container types.

Neptune define three container types: Atom, Series, and Set.

Detailed documentation on creating / modifying / reading variables of
a container type, refer to the documentation for the given container type,
e.g.

>>> help(Series)
"""

import time
from datetime import datetime


def parse_path(path):
    """
    path: string

    Returns: list of strings

    Throws: ValueError if invalid path
    """
    # TODO validate
    return path.split('/')


def path_to_str(path):
    return '/'.join(path)

###################################
### Variables and structures
###################################


class Variable:

    def __init__(self, experiment, path, typ):
        super().__init__()
        self._experiment = experiment
        self._path = path
        self._type = typ
        self._metadata = {}

    def type(self):
        return self._type

    def add_metadata(self, key, value):
        self._metadata[key] = value

    def _log(self, string):
        self._experiment._log(f'{path_to_str(self._path)}: {string}')


class Atom(Variable):
    """
    Modifying / initializing methods: assign
    Reading methods: read
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
        self._log(f'assign {value}')
        self._value = value
        self._type = typ


class Series(Variable):
    """
    Writing / initializing methods: log
    Reading methods: tail, all
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
        self._log(f'log step={step}, timestamp={timestamp}, value={value}')
        self._values.append((step, timestamp, value))

    # def tail(self, n_last, with_steps=False, with_timestamps=False):
    #    # TODO handle steps and timestamps
    #    return [v for _, _, v in self._values[-n_last:]]

    # def all(self, with_steps=False, with_timestamps=False):
    #    # TODO handle steps and timestamps
    #    return [v for _, _, v in self._values]


class Set(Variable):
    """
    Modifying / initializing methods: add
    Writing methods: remove, reset
    Reading methods: get
    """

    def __init__(self, experiment, path, typ):
        # TODO check that the type is supported by the Set structure
        super().__init__(experiment, path, typ)
        self._values = set()

    def get(self):
        return self._values

    def reset(self, *values):
        self._log(f'reset {values}')
        self._values = set(values)

    def add(self, *values):
        self._log(f'add {values}')
        self._values.update(values)

    def remove(self, *values):
        self._log(f'remove {values}')
        self._values.difference_update(values)
