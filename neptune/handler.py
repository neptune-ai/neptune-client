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

from neptune.internal.utils import parse_path, path_to_str
from neptune.variable import FloatVariable, StringVariable, FloatSeriesVariable, StringSeriesVariable, StringSetVariable

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
        var = self._experiment._structure.get(self._path)
        if var:
            return getattr(var, attr)
        else:
            raise AttributeError()

    def assign(self, value) -> None:
        var = self._experiment._structure.get(self._path)
        if not var:
            if isinstance(value, (float, int)):
                var = FloatVariable(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
            if isinstance(value, str):
                var = StringVariable(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
        var.assign(value)

    def log(self, value, step=None, timestamp=None) -> None:
        var = self._experiment._structure.get(self._path)
        if not var:
            if isinstance(value, (float, int)):
                var = FloatSeriesVariable(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
            if isinstance(value, str):
                var = StringSeriesVariable(self._experiment, self._path)
                self._experiment._structure.set(self._path, var)
        var.log(value, step, timestamp)

    def insert(self, *values) -> None:
        var = self._experiment._structure.get(self._path)
        if not var:
            var = StringSetVariable(self._experiment, self._path)
            self._experiment._structure.set(self._path, var)
        var.insert(list(values))

    def pop(self, path: str) -> None:
        self._experiment.pop(path_to_str(self._path) + "/" + path)

    def __delitem__(self, path) -> None:
        self.pop(path)
