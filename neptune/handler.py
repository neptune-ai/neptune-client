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

from typing import List, TYPE_CHECKING

from neptune.internal.utils.paths import parse_path, path_to_str
from neptune.variables.atoms.float import Float
from neptune.variables.atoms.string import String
from neptune.variables.series.float_series import FloatSeries
from neptune.variables.series.string_series import StringSeries
from neptune.variables.sets.string_set import StringSet

if TYPE_CHECKING:
    from neptune.experiment import Experiment


class Handler:

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__()
        self._experiment = _experiment
        self._path = path

    def __getitem__(self, path: str) -> 'Handler':
        return Handler(self._experiment, self._path + parse_path(path))

    def __setitem__(self, key: str, value) -> None:
        self[key].assign(value)

    def __getattr__(self, attr):
        # pylint: disable=protected-access
        var = self._experiment._structure.get(self._path)
        if var:
            return getattr(var, attr)
        else:
            raise AttributeError()

    def assign(self, value, wait: bool = False) -> None:
        # pylint: disable=protected-access
        var = self._experiment._structure.get(self._path)
        if not var:
            if isinstance(value, (float, int)):
                var = Float(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
            if isinstance(value, str):
                var = String(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
        var.assign(value, wait)

    def log(self, value, step=None, timestamp=None, wait: bool = False) -> None:
        # pylint: disable=protected-access
        var = self._experiment._structure.get(self._path)
        if not var:
            if isinstance(value, (float, int)):
                var = FloatSeries(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
            if isinstance(value, str):
                var = StringSeries(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
        var.log(value, step, timestamp, wait)

    def add(self, *values, wait: bool = False) -> None:
        # pylint: disable=protected-access
        var = self._experiment._structure.get(self._path)
        if not var:
            var = StringSet(self._experiment, self._path)
            self._experiment._structure.set(self._path, var)
        var.add(list(values), wait)

    def pop(self, path: str, wait: bool = False) -> None:
        self._experiment.pop(path_to_str(self._path) + "/" + path, wait)

    def __delitem__(self, path) -> None:
        self.pop(path)
