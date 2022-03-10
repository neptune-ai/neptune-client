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
from functools import wraps
from typing import Optional, TYPE_CHECKING, Union, Iterable, List


# backwards compatibility
# pylint: disable=unused-import
from neptune.new.exceptions import NeptuneException

from neptune.new.attributes import File
from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.attributes.constants import SYSTEM_STAGE_ATTRIBUTE_PATH
from neptune.new.attributes.file_set import FileSet
from neptune.new.attributes.namespace import Namespace
from neptune.new.attributes.series import FileSeries
from neptune.new.attributes.series.float_series import FloatSeries
from neptune.new.attributes.series.string_series import StringSeries
from neptune.new.attributes.sets.string_set import StringSet
from neptune.new.exceptions import (
    MissingFieldException,
    NeptuneCannotChangeStageManually,
)
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.utils import (
    verify_type,
    is_collection,
    verify_collection_type,
    is_float,
    is_string,
    is_float_like,
    is_string_like,
)
from neptune.new.internal.utils.paths import join_paths, parse_path
from neptune.new.types.atoms.file import File as FileVal
from neptune.new.types.value_copy import ValueCopy

if TYPE_CHECKING:
    from neptune.new.metadata_containers import MetadataContainer


def validate_path_not_protected(target_path: str, handler: "Handler"):
    # pylint: disable=protected-access
    path_protection_exception = handler._PROTECTED_PATHS.get(target_path)
    if path_protection_exception:
        raise path_protection_exception(target_path)


def check_protected_paths(fun):
    @wraps(fun)
    def inner_fun(self: "Handler", *args, **kwargs):
        # pylint: disable=protected-access
        validate_path_not_protected(self._path, self)
        return fun(self, *args, **kwargs)

    return inner_fun


class Handler:
    # paths which can't be modified by client directly
    _PROTECTED_PATHS = {
        SYSTEM_STAGE_ATTRIBUTE_PATH: NeptuneCannotChangeStageManually,
    }

    def __init__(self, container: "MetadataContainer", path: str):
        super().__init__()
        self._container = container
        self._path = path

    def __repr__(self):
        attr = self._container.get_attribute(self._path)
        formal_type = type(attr).__name__ if attr else "Unassigned"
        return f'<{formal_type} field at "{self._path}">'

    def _ipython_key_completions_(self):
        # pylint: disable=protected-access
        return self._container._get_subpath_suggestions(path_prefix=self._path)

    def __getitem__(self, path: str) -> "Handler":
        return Handler(self._container, join_paths(self._path, path))

    def __setitem__(self, key: str, value) -> None:
        self[key].assign(value)

    def _get_attribute(self):
        """Returns Attribute defined in `self._path` or throws MissingFieldException"""
        attr = self._container.get_attribute(self._path)
        if attr is None:
            raise MissingFieldException(self._path)
        return attr

    @property
    def container(self) -> "MetadataContainer":
        """Returns the container that the attribute is attached to"""
        return self._container

    @check_protected_paths
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
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr:
                if isinstance(value, Handler):
                    value = ValueCopy(value)
                attr.process_assignment(value, wait)
            else:
                self._container.define(self._path, value, wait)

    @check_protected_paths
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
           https://docs.neptune.ai/api-reference/field-types#.upload

        """
        value = FileVal.create_from(value)

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if not attr:
                attr = File(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.upload(value, wait)

    @check_protected_paths
    def upload_files(
        self, value: Union[str, Iterable[str]], wait: bool = False
    ) -> None:
        if is_collection(value):
            verify_collection_type("value", value, str)
        else:
            verify_type("value", value, str)

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if not attr:
                attr = FileSet(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.upload_files(value, wait)

    @check_protected_paths
    def log(
        self,
        value,
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
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

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if not attr:
                if is_collection(value):
                    if value:
                        first_value = next(iter(value))
                    else:
                        raise ValueError(
                            "Cannot deduce value type: `value` cannot be empty"
                        )
                else:
                    first_value = value

                if is_float(first_value):
                    attr = FloatSeries(self._container, parse_path(self._path))
                elif is_string(first_value):
                    attr = StringSeries(self._container, parse_path(self._path))
                elif FileVal.is_convertable(first_value):
                    attr = FileSeries(self._container, parse_path(self._path))
                elif is_float_like(first_value):
                    attr = FloatSeries(self._container, parse_path(self._path))
                elif is_string_like(first_value):
                    attr = StringSeries(self._container, parse_path(self._path))
                else:
                    raise TypeError(
                        "Value of unsupported type {}".format(type(first_value))
                    )

                self._container.set_attribute(self._path, attr)
            attr.log(value, step=step, timestamp=timestamp, wait=wait, **kwargs)

    @check_protected_paths
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
           https://docs.neptune.ai/api-reference/field-types#.add
        """
        verify_type("values", values, (str, Iterable))
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if not attr:
                attr = StringSet(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.add(values, wait)

    @check_protected_paths
    def pop(self, path: str = None, wait: bool = False) -> None:
        # pylint: disable=protected-access
        with self._container.lock():
            handler = self
            if path:
                verify_type("path", path, str)
                handler = self[path]
                path = join_paths(self._path, path)
                # extra check: check_protected_paths decorator does not catch flow with non-null path
                validate_path_not_protected(path, self)
            else:
                path = self._path

            attribute = self._container.get_attribute(path)
            if isinstance(attribute, Namespace):
                for child_path in list(attribute):
                    handler.pop(child_path, wait)
            else:
                self._container._pop_impl(parse_path(path), wait)

    @check_protected_paths
    def remove(self, values: Union[str, Iterable[str]], wait: bool = False) -> None:
        """Removes the provided tag or tags from the set.

        Args:
            values (str or collection of str): Tag or tags to be removed.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        You may also want to check `remove docs page`_.

        .. _remove docs page:
           https://docs.neptune.ai/api-reference/field-types#.remove
        """
        return self._pass_call_to_attr(function_name="remove", values=values, wait=wait)

    @check_protected_paths
    def clear(self, wait: bool = False):
        """Removes all tags from the `StringSet`.

        Args:
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        You may also want to check `clear docs page`_.

        .. _clear docs page:
           https://docs.neptune.ai/api-reference/field-types#.clear
        """
        return self._pass_call_to_attr(function_name="clear", wait=wait)

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
        return self._pass_call_to_attr(function_name="fetch")

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
        return self._pass_call_to_attr(function_name="fetch_last")

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
        return self._pass_call_to_attr(
            function_name="fetch_values", include_timestamp=include_timestamp
        )

    @check_protected_paths
    def delete_files(
        self, paths: Union[str, Iterable[str]], wait: bool = False
    ) -> None:
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
            https://docs.neptune.ai/api-reference/field-types#.delete_files
        """
        return self._pass_call_to_attr(
            function_name="delete_files", paths=paths, wait=wait
        )

    @check_protected_paths
    def download(self, destination: str = None) -> None:
        """Downloads the stored file or files to the working directory or specified destination.

        Available for following field types (`Field types docs page`_):
            * `File`
            * `FileSeries`
            * `FileSet`
            * `Artifact`

        Args:
            destination (str, optional): Path to where the file(s) should be downloaded.
                If `None` file will be downloaded to the working directory.
                If `destination` is a directory, the file will be downloaded to the specified directory with a filename
                composed from field name and extension (if present).
                If `destination` is a path to a file, the file will be downloaded under the specified name.
                Defaults to `None`.

        .. _Field types docs page:
           https://docs.neptune.ai/api-reference/field-types
        """
        return self._pass_call_to_attr(
            function_name="download", destination=destination
        )

    def download_last(self, destination: str = None) -> None:
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
           https://docs.neptune.ai/api-reference/field-types#.download_last
        """
        return self._pass_call_to_attr(
            function_name="download_last", destination=destination
        )

    def fetch_hash(self) -> str:
        """Fetches the hash of an artifact.

        You may also want to check `fetch_hash docs page`_.
           https://docs.neptune.ai/api-reference/field-types#.fetch_hash
        """
        return self._pass_call_to_attr(function_name="fetch_hash")

    def fetch_files_list(self) -> List[ArtifactFileData]:
        """Fetches the list of files in an artifact and their metadata.

        You may also want to check `fetch_files_list docs page`_.
           https://docs.neptune.ai/api-reference/field-types#.fetch_files_list
        """
        return self._pass_call_to_attr(function_name="fetch_files_list")

    def _pass_call_to_attr(self, function_name, **kwargs):
        return getattr(self._get_attribute(), function_name)(**kwargs)

    @check_protected_paths
    def track_files(
        self, path: str, destination: str = None, wait: bool = False
    ) -> None:
        """Creates an artifact tracking some files.

        You may also want to check `track_files docs page`_.
           https://docs.neptune.ai/api-reference/field-types#.track_files
        """
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if not attr:
                attr = Artifact(self._container, parse_path(self._path))

            self._container.set_attribute(self._path, attr)

            attr.track_files(path=path, destination=destination, wait=wait)

    def __delitem__(self, path) -> None:
        self.pop(path)
