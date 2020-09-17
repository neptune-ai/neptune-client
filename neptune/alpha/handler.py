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

from typing import TYPE_CHECKING, Union

from neptune.alpha.internal.utils import verify_type, verify_collection_type

from neptune.alpha.types.value import Value
from neptune.alpha.types.atoms.file import File

from neptune.alpha.types.sets.string_set import StringSet

from neptune.alpha.types.series.string_series import StringSeries

from neptune.alpha.types.series.float_series import FloatSeries

from neptune.alpha.internal.utils.paths import join_paths

if TYPE_CHECKING:
    from neptune.alpha.experiment import Experiment


class Handler:

    def __init__(self, _experiment: 'Experiment', path: str):
        super().__init__()
        self._experiment = _experiment
        self._path = path

    def __getitem__(self, path: str) -> 'Handler':
        return Handler(self._experiment, join_paths(self._path, path))

    def __setitem__(self, key: str, value) -> None:
        self[key].assign(value)

    def __getattr__(self, attribute_name):
        attr = self._experiment.get_attribute(self._path)
        if attr:
            return getattr(attr, attribute_name)
        else:
            raise AttributeError()

    def assign(self, value: Union[Value, int, float, str], wait: bool = False) -> None:
        verify_type("value", value, (Value, int, float, str))
        with self._experiment.lock():
            attr = self._experiment.get_attribute(self._path)
            if attr:
                attr.assign(value, wait)
            else:
                self._experiment.define(self._path, value, wait)

    def save(self, value: str, wait: bool = False) -> None:
        self.assign(File(value), wait)

    # TODO: Support Image value
    def log(self, value: Union[int, float, str], step=None, timestamp=None, wait: bool = False) -> None:
        verify_type("value", value, (int, float, str))
        verify_type("step", step, (int, float, type(None)))
        verify_type("timestamp", step, (int, float, type(None)))
        with self._experiment.lock():
            attr = self._experiment.get_attribute(self._path)
            if attr:
                attr.log(value, step=step, timestamp=timestamp, wait=wait)
            else:
                if isinstance(value, (float, int)):
                    val = FloatSeries([value])
                elif isinstance(value, str):
                    val = StringSeries([value])
                self._experiment.define(self._path, val, wait)

    def add(self, *values: str, wait: bool = False) -> None:
        verify_collection_type("value", values, str)
        with self._experiment.lock():
            attr = self._experiment.get_attribute(self._path)
            if attr:
                attr.add(values, wait)
            else:
                val = StringSet(values)
                self._experiment.define(self._path, val, wait)

    def pop(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        self._experiment.pop(join_paths(self._path, path), wait)

    def __delitem__(self, path) -> None:
        self.pop(path)
