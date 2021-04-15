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
from typing import Optional, TYPE_CHECKING, Union, Iterable

from neptune.new.attributes import File
from neptune.new.attributes.file_set import FileSet
from neptune.new.attributes.series import FileSeries
from neptune.new.attributes.series.float_series import FloatSeries
from neptune.new.attributes.series.string_series import StringSeries
from neptune.new.attributes.sets.string_set import StringSet
from neptune.new.internal.utils import verify_type, is_collection, verify_collection_type, is_float, is_string, \
    is_float_like, is_string_like
from neptune.new.internal.utils.paths import join_paths, parse_path
from neptune.new.types.atoms.file import File as FileVal

if TYPE_CHECKING:
    from neptune.new.run import Run


class Handler:

    def __init__(self, run: 'Run', path: str):
        super().__init__()
        self._run = run
        self._path = path

    def __getitem__(self, path: str) -> 'Handler':
        return Handler(self._run, join_paths(self._path, path))

    def __setitem__(self, key: str, value) -> None:
        self[key].assign(value)

    def __getattr__(self, attribute_name):
        attr = self._run.get_attribute(self._path)
        if attr:
            return getattr(attr, attribute_name)
        else:
            raise AttributeError()

    def assign(self, value, wait: bool = False) -> None:
        if not isinstance(value, dict):
            return self._assign_impl(value, wait)
        for key, value in value.items():
            self[key].assign(value, wait)

    def _assign_impl(self, value, wait: bool = False) -> None:
        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if attr:
                attr.assign(value, wait)
            else:
                self._run.define(self._path, value, wait)

    def upload(self, value, wait: bool = False) -> None:
        value = FileVal.create_from(value)

        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if not attr:
                attr = File(self._run, parse_path(self._path))
                attr.upload(value, wait)
                self._run.set_attribute(self._path, attr)
            else:
                attr.upload(value, wait)

    def upload_files(self, value: Union[str, Iterable[str]], wait: bool = False) -> None:
        if is_collection(value):
            verify_collection_type("value", value, str)
        else:
            verify_type("value", value, str)

        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if not attr:
                attr = FileSet(self._run, parse_path(self._path))
                attr.upload_files(value, wait)
                self._run.set_attribute(self._path, attr)
            else:
                attr.upload_files(value, wait)

    def log(self,
            value,
            step: Optional[float] = None,
            timestamp: Optional[float] = None,
            wait: bool = False,
            **kwargs) -> None:
        verify_type("step", step, (int, float, type(None)))
        verify_type("timestamp", timestamp, (int, float, type(None)))

        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if not attr:
                if is_collection(value):
                    if value:
                        first_value = next(iter(value))
                    else:
                        raise ValueError("Cannot deduce value type: `value` cannot be empty")
                else:
                    first_value = value

                if is_float(first_value):
                    attr = FloatSeries(self._run, parse_path(self._path))
                elif is_string(first_value):
                    attr = StringSeries(self._run, parse_path(self._path))
                elif FileVal.is_convertable(first_value):
                    attr = FileSeries(self._run, parse_path(self._path))
                elif is_float_like(first_value):
                    attr = FloatSeries(self._run, parse_path(self._path))
                elif is_string_like(first_value):
                    attr = StringSeries(self._run, parse_path(self._path))
                else:
                    raise TypeError("Value of unsupported type {}".format(type(first_value)))

                attr.log(value, step=step, timestamp=timestamp, wait=wait, **kwargs)
                self._run.set_attribute(self._path, attr)
            else:
                attr.log(value, step=step, timestamp=timestamp, wait=wait, **kwargs)

    def add(self, values: Union[str, Iterable[str]], wait: bool = False) -> None:
        verify_type("values", values, (str, Iterable))
        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if not attr:
                attr = StringSet(self._run, parse_path(self._path))
                attr.add(values, wait)
                self._run.set_attribute(self._path, attr)
            else:
                attr.add(values, wait)

    def pop(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        self._run.pop(join_paths(self._path, path), wait)

    def __delitem__(self, path) -> None:
        self.pop(path)
