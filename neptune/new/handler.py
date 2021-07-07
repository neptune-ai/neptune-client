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
from neptune.new.exceptions import MissingFieldException, NeptuneException
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
            raise MissingFieldException(self._path)

    def __getattribute__(self, attribute_name):
        _docstring_attrs = super().__getattribute__('DOCSTRING_ATTRIBUTES')
        if attribute_name in _docstring_attrs:
            raise AttributeError()
        return super().__getattribute__(attribute_name)

    def assign(self, value, wait: bool = False) -> None:
        """Assigns the provided value to the field.

        Available for following field types (`Field types docs page`_):
            * `Integer`
            * `Float`
            * `Boolean`
            * `String`

        Args:
            value: Value to be stored in a field.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `None`.

        Examples:
            Assigning values:

            >>> import neptune.new as neptune
            >>> run = neptune.init()

            >>> # You can both use the Python assign operator (=)
            ... run['parameters/max_epochs'] = 5
            >>> # as well as directly use the .assign method
            ... run['parameters/max_epochs'].assign(5)

            You can assign integers, floats, bools, strings

            >>> run['parameters/max_epochs'] = 5
            >>> run['parameters/max_lr'] = 0.4
            >>> run['parameters/early_stopping'] = True
            >>> run['JIRA'] = 'NPT-952'

            You can also assign values in batch through a dict

            >>> params = {'max_epochs': 5, 'lr': 0.4}
            >>> run['parameters'] = params

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if attr:
                attr.assign(value, wait)
            else:
                self._run.define(self._path, value, wait)

    def upload(self, value, wait: bool = False) -> None:
        """Uploads provided file under specified field path.

        Args:
            value (str or File): Path to the file to be uploaded or `File` value object.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `False`.

        Examples:
            >>> import neptune.new as neptune
            >>> run = neptune.init()

            >>> # Upload example data
            ... run["dataset/data_sample"].upload("sample_data.csv")

            >>> # Both the content and the extension is stored
            ... # When downloaded the filename is a combination of path and the extension
            ... run["dataset/data_sample"].download() # data_sample.csv

            Explicitely create File value object

            >>> from neptune.new.types import File
            >>> run["dataset/data_sample"].upload(File("sample_data.csv"))

        You may also want to check `upload docs page`_.

        .. _upload docs page:
           https://docs.neptune.ai/api-reference/field-types#upload

        """
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
        """Logs the provided value or a collection of values.

        Available for following field types (`Field types docs page`_):
            * `FloatSeries`
            * `StringSeries`
            * `FileSeries`


        Args:
            value: Value or collection of values to be added to the field.
            step (float or int, optional, default is None): Index of the log entry being appended.
                Must be strictly increasing.
                Defaults to `None`.
            timestamp(float or int, optional): Time index of the log entry being appended in form of Unix time.
                If `None` current time (`time.time()`) will be used as a timestamp.
                Defaults to `None`.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `False`.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types

        """
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
        """Adds the provided tag or tags to the run's tags.

        Args:
            values (str or collection of str): Tag or tags to be added.
                .. note::
                    If you want you can use emojis in your tags eg. "Exploration 🧪"
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        You may also want to check `add docs page`_.

        .. _add types docs page:
           https://docs.neptune.ai/api-reference/field-types#add
        """
        verify_type("values", values, (str, Iterable))
        with self._run.lock():
            attr = self._run.get_attribute(self._path)
            if not attr:
                attr = StringSet(self._run, parse_path(self._path))
                attr.add(values, wait)
                self._run.set_attribute(self._path, attr)
            else:
                attr.add(values, wait)

    def pop(self, path: str = None, wait: bool = False) -> None:
        if path:
            verify_type("path", path, str)
            self._run.pop(join_paths(self._path, path), wait)
        else:
            self._run.pop(self._path, wait)

    # Following attributes are implemented only for docstring hints and autocomplete
    DOCSTRING_ATTRIBUTES = [
        'remove',
        'clear',
        'fetch',
        'fetch_last',
        'fetch_values',
        'delete_files',
        'download',
        'download_last',
    ]

    def remove(self, values: Union[str, Iterable[str]], wait: bool = False) -> None:
        """Removes the provided tag or tags from the set.

        Args:
            values (str or collection of str): Tag or tags to be removed.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        You may also want to check `remove docs page`_.

        .. _remove docs page:
           https://docs.neptune.ai/api-reference/field-types#remove
        """
        raise NeptuneException('Should be never called.')

    def clear(self, wait: bool = False):
        """Removes all tags from the `StringSet`.

        Args:
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        You may also want to check `clear docs page`_.

        .. _clear docs page:
           https://docs.neptune.ai/api-reference/field-types#clear
        """
        raise NeptuneException('Should be never called.')

    def fetch(self):
        """Fetches fields value or in case of a namespace fetches values of all non-File Atom fields as a dictionary.

        Available for following field types (`Field types docs page`_):
            * `Integer`
            * `Float`
            * `Boolean`
            * `String`
            * `DateTime`
            * `StringSet`
            * `Namespace handler`

        Returns:
            Value stored in the field or in case of a namespace a dictionary containing all non-Atom fields values.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        raise NeptuneException('Should be never called.')

    def fetch_last(self):
        """Fetches last value stored in the series from Neptune servers.

        Available for following field types (`Field types docs page`_):
            * `FloatSeries`
            * `StringSeries`

        Returns:
            Fetches last value stored in the series from Neptune servers.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        raise NeptuneException('Should be never called.')

    def fetch_values(self, include_timestamp: Optional[bool] = True):
        """Fetches all values stored in the series from Neptune servers.

        Available for following field types (`Field types docs page`_):
            * `FloatSeries`
            * `StringSeries`

        Args:
            include_timestamp (bool, optional): Whether the fetched data should include the timestamp field.
                Defaults to `True`.

        Returns:
            ``Pandas.DataFrame``: containing all the values and their indexes stored in the series field.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        raise NeptuneException('Should be never called.')

    def delete_files(self, paths: Union[str, Iterable[str]], wait: bool = False) -> None:
        """Delete the file or files specified by paths from the `FileSet` stored on the Neptune servers.

        Args:
            paths (str or collection of str): `Path` or paths to files or folders to be deleted.
                Note that these are paths relative to the FileSet itself e.g. if the `FileSet` contains
                file `example.txt`, `varia/notes.txt`, `varia/data.csv` to delete whole subfolder you would pass
                varia as the argument.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `None`.

        You may also want to check `delete_files docs page`_.

        .. _delete_files docs page:
            https://docs.neptune.ai/api-reference/field-types#delete_files
        """
        raise NeptuneException('Should be never called.')

    def download(self, destination: str = None, wait: bool = True) -> None:
        """Downloads the stored file or files to the working directory or specified destination.

        Available for following field types (`Field types docs page`_):
            * `File`
            * `FileSeries`
            * `FileSet`

        Args:
            destination (str, optional): Path to where the file(s) should be downloaded.
                If `None` file will be downloaded to the working directory.
                If `destination` is a directory, the file will be downloaded to the specified directory with a filename
                composed from field name and extension (if present).
                If `destination` is a path to a file, the file will be downloaded under the specified name.
                Defaults to `None`.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `None`.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        raise NeptuneException('Should be never called.')

    def download_last(self, destination: str = None, wait: bool = True) -> None:
        """Downloads the stored file or files to the working directory or specified destination.

        Args:
            destination (str, optional): Path to where the file(s) should be downloaded.
                If `None` file will be downloaded to the working directory.
                If `destination` is a directory, the file will be downloaded to the specified directory with a filename
                composed from field name and extension (if present).
                If `destination` is a path to a file, the file will be downloaded under the specified name.
                Defaults to `None`.

        You may also want to check `download_last docs page`_.

        .. _download_last docs page:
           https://docs.neptune.ai/api-reference/field-types#download_last
        """
        raise NeptuneException('Should be never called.')

    def __delitem__(self, path) -> None:
        self.pop(path)
