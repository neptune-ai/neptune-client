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

import time
from typing import List, TYPE_CHECKING

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import AssignFloat, AssignString, LogFloats, LogStrings, AddStrings, RemoveStrings, \
    ClearFloatLog, ClearStringLog, ClearStringSet
from neptune.types.atoms.float import Float
from neptune.types.atoms.string import String
from neptune.types.sets.string_set import StringSet

if TYPE_CHECKING:
    from neptune.experiment import Experiment


class Variable:

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__()
        self._experiment = _experiment
        self._path = path


class AtomVariable(Variable):
    pass


class FloatVariable(AtomVariable):

    def assign(self, value: float, wait: bool = False):
        self._experiment._op_processor.enqueue_operation(AssignFloat(self._experiment._uuid, self._path, value), wait)

    def get(self):
        val = self._experiment._backend.get(self._experiment._uuid, self._path)
        if not isinstance(val, Float):
            raise MetadataInconsistency("Variable {} is not a Float".format(self._path))
        return val.value


class StringVariable(AtomVariable):

    def assign(self, value: str, wait: bool = False):
        self._experiment._op_processor.enqueue_operation(AssignString(self._experiment._uuid, self._path, value), wait)

    def get(self):
        val = self._experiment._backend.get(self._experiment._uuid, self._path)
        if not isinstance(val, String):
            raise MetadataInconsistency("Variable {} is not a String".format(self._path))
        return val.value


class FloatSeriesVariable(Variable):

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__(_experiment, path)
        self._next_step = 0

    def log(self, value: float, step: float = None, timestamp: float = None, wait: bool = False):
        # TODO: Support steps and timestamps
        if not step:
            step = self._next_step
        if not timestamp:
            timestamp = time.time()
        self._next_step = step + 1

        self._experiment._op_processor.enqueue_operation(LogFloats(self._experiment._uuid, self._path, [value]), wait)

    def clear(self, wait: bool = False):
        self._experiment.queue_operation(ClearFloatLog(self._experiment._uuid, self._path), wait)


class StringSeriesVariable(Variable):

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__(_experiment, path)
        self._next_step = 0

    def log(self, value: str, step: float = None, timestamp: float = None, wait: bool = False):
        if not step:
            step = self._next_step
        if not timestamp:
            timestamp = time.time()
        self._next_step = step + 1

        self._experiment._op_processor.enqueue_operation(LogStrings(self._experiment._uuid, self._path, [value]), wait)

    def clear(self, wait: bool = False):
        self._experiment.queue_operation(ClearStringLog(self._experiment._uuid, self._path), wait)


class StringSetVariable(Variable):

    def add(self, values: List[str], wait: bool = False):
        self._experiment._op_processor.enqueue_operation(
            AddStrings(self._experiment._uuid, self._path, values), wait)

    def remove(self, values: List[str], wait: bool = False):
        self._experiment._op_processor.enqueue_operation(
            RemoveStrings(self._experiment._uuid, self._path, values), wait)

    def clear(self, wait: bool = False):
        self._experiment._op_processor.enqueue_operation(ClearStringSet(self._experiment._uuid, self._path), wait)

    def get(self):
        val = self._experiment._backend.get(self._experiment._uuid, self._path)
        if  not isinstance(val, StringSet):
            raise MetadataInconsistency("Variable {} is not a StringSet".format(self._path))
        return val.values
