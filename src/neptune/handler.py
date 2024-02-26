#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["Handler"]

from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    NewType,
    Optional,
    Union,
)

from neptune.api.dtos import FileEntry
from neptune.attributes import File
from neptune.attributes.atoms.artifact import Artifact
from neptune.attributes.constants import SYSTEM_STAGE_ATTRIBUTE_PATH
from neptune.attributes.file_set import FileSet
from neptune.attributes.namespace import Namespace
from neptune.attributes.series import FileSeries
from neptune.attributes.series.float_series import FloatSeries
from neptune.attributes.series.string_series import StringSeries
from neptune.attributes.sets.string_set import StringSet
from neptune.common.warnings import warn_about_unsupported_type
from neptune.exceptions import (
    MissingFieldException,
    NeptuneCannotChangeStageManually,
    NeptuneUserApiInputException,
)
from neptune.internal.artifacts.types import ArtifactFileData
from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import (
    is_collection,
    is_dict_like,
    is_float,
    is_float_like,
    is_string,
    is_stringify_value,
    verify_collection_type,
    verify_type,
)
from neptune.internal.utils.paths import (
    join_paths,
    parse_path,
)
from neptune.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.metadata_containers.abstract import SupportsNamespaces
from neptune.types.atoms.file import File as FileVal
from neptune.types.type_casting import cast_value_for_extend
from neptune.types.value_copy import ValueCopy
from neptune.typing import ProgressBarType
from neptune.utils import stringify_unsupported

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer

    NeptuneObject = NewType("NeptuneObject", MetadataContainer)


def validate_path_not_protected(target_path: str, handler: "Handler"):
    path_protection_exception = handler._PROTECTED_PATHS.get(target_path)
    if path_protection_exception:
        raise path_protection_exception(target_path)


def check_protected_paths(fun):
    @wraps(fun)
    def inner_fun(self: "Handler", *args, **kwargs):
        validate_path_not_protected(self._path, self)
        return fun(self, *args, **kwargs)

    return inner_fun


ExtendDictT = Union[Collection[Any], Dict[str, "ExtendDictT"]]


class Handler(SupportsNamespaces):
    # paths which can't be modified by client directly
    _PROTECTED_PATHS = {
        SYSTEM_STAGE_ATTRIBUTE_PATH: NeptuneCannotChangeStageManually,
    }

    def __init__(self, container: "MetadataContainer", path: str):
        super().__init__()
        self._container = container
        self._path = str(path)

    def __repr__(self):
        attr = self._container.get_attribute(self._path)
        formal_type = type(attr).__name__ if attr else "Unassigned"
        return f'<{formal_type} field at "{self._path}">'

    def _ipython_key_completions_(self):
        return self._container._get_subpath_suggestions(path_prefix=self._path)

    def __getitem__(self, path: str) -> "Handler":
        return Handler(self._container, join_paths(self._path, path))

    def __setitem__(self, key: str, value) -> None:
        self[key].assign(value)

    def __getattr__(self, item: str):
        run_level_methods = {"exists", "get_structure", "print_structure", "stop", "sync", "wait"}

        if item in run_level_methods:
            raise AttributeError(
                "You're invoking an object-level method on a handler for a namespace" "inside the object.",
                f"""
                                 For example: You're trying run[{self._path}].{item}()
                                 but you probably want run.{item}().

                                 To obtain the root object of the namespace handler, you can do the following:
                                 root_run = run[{self._path}].get_root_object()
                                 root_run.{item}()
                                """,
            )

        return object.__getattribute__(self, item)

    def _get_attribute(self):
        """Returns an attribute defined in `self._path` or throws MissingFieldException."""
        attr = self._container.get_attribute(self._path)
        if attr is None:
            raise MissingFieldException(self._path)
        return attr

    @property
    def container(self) -> "MetadataContainer":
        """Returns the container that the attribute is attached to."""
        return self._container

    def get_root_object(self) -> "NeptuneObject":
        """Returns the root-level object of a namespace handler.

        Example:
            If you use it on the namespace of a run, the run object is returned.

            >>> pretraining = run["workflow/steps/pretraining"]
            >>> pretraining.stop()
            ... # Error: pretraining is a namespace handler object, not a run object
            >>> pretraining_run = pretraining.get_root_object()
            >>> pretraining_run.stop()  # The root run is stopped

        For more information, see the docs:
        https://docs.neptune.ai/api/field_types/#get_root_object
        """
        return self._container

    @check_protected_paths
    def assign(self, value, *, wait: bool = False) -> None:
        """Assigns the provided value to the field.

        Available for the following field types:
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

            >>> import neptune
            >>> run = neptune.init_run()

            >>> # You can use both the Python assign operator (=)
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

        For more information, see the docs:
           https://docs.neptune.ai/api-reference/field-types
        """
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                self._container.define(self._path, value)
            else:
                if isinstance(value, Handler):
                    value = ValueCopy(value)
                attr.process_assignment(value, wait=wait)

    @check_protected_paths
    def upload(self, value, *, wait: bool = False) -> None:
        """Uploads the provided file under the specified field path.

        Args:
            value (str or File): Path to the file to be uploaded or `File` value object.
            wait (bool, optional): If `True` the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `False`.

        Examples:
            >>> import neptune
            >>> run = neptune.init_run()

            >>> # Upload example data
            ... run["dataset/data_sample"].upload("sample_data.csv")

            >>> # Both the content and the extension is stored
            ... # When downloaded the filename is a combination of path and the extension
            ... run["dataset/data_sample"].download() # data_sample.csv

            Explicitly create File value object

            >>> from neptune.types import File
            >>> run["dataset/data_sample"].upload(File("sample_data.csv"))

        For more information, see the docs:
           https://docs.neptune.ai/api/field_types#upload

        """
        value = FileVal.create_from(value)

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                attr = File(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.upload(value, wait=wait)

    @check_protected_paths
    def upload_files(self, value: Union[str, Iterable[str]], *, wait: bool = False) -> None:
        if is_collection(value):
            verify_collection_type("value", value, str)
        else:
            verify_type("value", value, str)

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                attr = FileSet(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.upload_files(value, wait=wait)

    @check_protected_paths
    def log(
        self,
        value,
        *,
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        """Logs the provided value or a collection of values.

        Available for the following field types:

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

        For more information, see the docs:
           https://docs.neptune.ai/api-reference/field-types

        """
        verify_type("step", step, (int, float, type(None)))
        verify_type("timestamp", timestamp, (int, float, type(None)))

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                from_stringify_value = False
                if is_stringify_value(value):
                    from_stringify_value, value = True, value.value

                if is_collection(value):
                    if value:
                        first_value = next(iter(value))
                    else:
                        raise ValueError("Cannot deduce value type: `value` cannot be empty")
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
                elif from_stringify_value:
                    if is_collection(value):
                        value = list(map(str, value))
                    else:
                        value = str(value)
                    attr = StringSeries(self._container, parse_path(self._path))
                else:
                    warn_about_unsupported_type(type_str=str(type(first_value)))
                    return None

                self._container.set_attribute(self._path, attr)
            attr.log(value, step=step, timestamp=timestamp, wait=wait, **kwargs)

    @check_protected_paths
    def append(
        self,
        value: Union[dict, Any],
        *,
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        """Logs a series of values, such as a metric, by appending the provided value to the end of the series.

        Available for following series field types:

            * `FloatSeries` - series of float values
            * `StringSeries` - series of strings
            * `FileSeries` - series of files

        When you log the first value, the type of the value determines what type of field is created.
        To learn more about field types, see the docs: https://docs.neptune.ai/api/field_types

        Args:
            value: Value to be added to the series field.
            step: Optional index of the entry being appended. Must be strictly increasing.
            timestamp: Optional time index of the log entry being appended, in Unix time format.
                If None, the current time (obtained with `time.time()`) is used.
            wait: If True, the client sends all tracked metadata to the server before executing the call.
                For more information, see: https://docs.neptune.ai/api/universal/#wait

        Examples:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> for epoch in range(n_epochs):
            ...     ... # Your training loop
            ...     run["train/epoch/loss"].append(loss)  # FloatSeries
            ...     token = str(...)
            ...     run["train/tokens"].append(token)  # StringSeries
            ...     run["train/distribution"].append(plt_histogram, step=epoch)  # FileSeries
        """
        verify_type("step", step, (int, float, type(None)))
        verify_type("timestamp", timestamp, (int, float, type(None)))
        if step is not None:
            step = [step]
        if timestamp is not None:
            timestamp = [timestamp]

        value = ExtendUtils.transform_to_extend_format(value)
        self.extend(value, steps=step, timestamps=timestamp, wait=wait, **kwargs)

    @check_protected_paths
    def extend(
        self,
        values: ExtendDictT,
        *,
        steps: Optional[Collection[float]] = None,
        timestamps: Optional[Collection[float]] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        """Logs a series of values by appending the provided collection of values to the end of the series.

        Available for the following series field types:

            * `FloatSeries` - series of float values
            * `StringSeries` - series of strings
            * `FileSeries` - series of files

        When you log the first value, the type of the value determines what type of field is created.
        To learn more about field types, see the docs: https://docs.neptune.ai/api/field_types

        Args:
            values: Values to be added to the series field, as a dictionary or collection.
            steps: Optional collection of indeces for the entries being appended. Must be strictly increasing.
            timestamps: Optional collection of time indeces for the entries being appended, in Unix time format.
                If None, the current time (obtained with `time.time()`) is used.
            wait: If True, the client sends all tracked metadata to the server before executing the call.
                For details, see https://docs.neptune.ai/api/universal/#wait

        Example:
            The following example reads a CSV file into a pandas DataFrame and extracts the values
            to create a Neptune series:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> for epoch in range(n_epochs):
            ...     df = pandas.read_csv("time_series.csv")
            ...     ys = df["value"]
            ...     ts = df["timestamp"]
            ...     run["data/example_series"].extend(ys, timestamps=ts)
        """
        ExtendUtils.validate_values_for_extend(values, steps, timestamps)

        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                neptune_value = cast_value_for_extend(values)
                if neptune_value is None:
                    warn_about_unsupported_type(type_str=str(type(values)))
                    return None

                attr = ValueToAttributeVisitor(self._container, parse_path(self._path)).visit(neptune_value)
                self._container.set_attribute(self._path, attr)

            attr.extend(values, steps=steps, timestamps=timestamps, wait=wait, **kwargs)

    @check_protected_paths
    def add(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
        """Adds the provided tags to the run.

        Args:
            values (str or collection of str): Tag or tags to be added.
                .. note::
                    You can use emojis in your tags. For example, "Exploration 🧪"
            wait (bool, optional): If `True`, the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        For more information, see the docs:
           https://docs.neptune.ai/api/field_types#add
        """
        verify_type("values", values, (str, Iterable))
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                attr = StringSet(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)
            attr.add(values, wait=wait)

    @check_protected_paths
    def pop(self, path: str = None, *, wait: bool = False) -> None:
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
                    handler.pop(child_path, wait=wait)
            else:
                self._container._pop_impl(parse_path(path), wait=wait)

    @check_protected_paths
    def remove(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
        """Removes the provided tags from the set.

        Args:
            values (str or collection of str): Tags to be removed.
            wait (bool, optional): If `True`, the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        For more information, see the docs:
           https://docs.neptune.ai/api/field_types#remove
        """
        return self._pass_call_to_attr(function_name="remove", values=values, wait=wait)

    @check_protected_paths
    def clear(self, *, wait: bool = False):
        """Removes all tags from the `StringSet`.

        Args:
            wait (bool, optional): If `True`, the client will wait to send all tracked metadata to the server first.
                This makes the call synchronous.
                Defaults to `False`.

        For more information, see the docs:
           https://docs.neptune.ai/api/field_types#clear
        """
        return self._pass_call_to_attr(function_name="clear", wait=wait)

    def fetch(self):
        """Fetches fields value or, in case of a namespace, fetches values of all non-File Atom fields as a dictionary.

        Available for the following field types:

            * `Integer`
            * `Float`
            * `Boolean`
            * `String`
            * `DateTime`
            * `StringSet`
            * `Namespace handler`

        Returns:
            If called on a field, returns the stored value.
            If called on a namespace, returns a dictionary containing the values of all non-Atom fields.

        For more information on field types, see the docs:
           https://docs.neptune.ai/api-reference/field-types
        """
        return self._pass_call_to_attr(function_name="fetch")

    def fetch_last(self):
        """Fetches the last value stored in the series from Neptune.

        Available for the following field types:

            * `FloatSeries`
            * `StringSeries`

        Returns:
            Fetches the last value stored in the series from Neptune.

        For more information on field types, see the docs:
           https://docs.neptune.ai/api-reference/field-types
        """
        return self._pass_call_to_attr(function_name="fetch_last")

    def fetch_values(self, *, include_timestamp: Optional[bool] = True, progress_bar: Optional[ProgressBarType] = None):
        """Fetches all values stored in the series from Neptune.

        Available for the following field types:

            * `FloatSeries`
            * `StringSeries`

        Args:
            include_timestamp (bool, optional): Whether the fetched data should include the timestamp field.
                Defaults to `True`.
            progress_bar: (bool or Type of progress bar, optional): progress bar to be used while fetching values.
                If `None` or `True` the default tqdm-based progress bar will be used.
                If `False` no progress bar will be used.
                If a type of progress bar is passed, it will be used instead of the default one.
                Defaults to `None`.

        Returns:
            ``Pandas.DataFrame``: containing all the values and their indexes stored in the series field.

        For more information on field types, see the docs:
           https://docs.neptune.ai/api-reference/field-types
        """
        return self._pass_call_to_attr(
            function_name="fetch_values",
            include_timestamp=include_timestamp,
            progress_bar=progress_bar,
        )

    @check_protected_paths
    def delete_files(self, paths: Union[str, Iterable[str]], *, wait: bool = False) -> None:
        """Deletes the files specified by the paths from the `FileSet` stored on the Neptune servers.

        Args:
            paths (str or collection of str): `Path` or paths to files or folders to be deleted.
                Note that the paths are relative to the FileSet itself. For example, if the `FileSet` contains
                the files `example.txt`, `varia/notes.txt`, `varia/data.csv`, to delete the entire varia subfolder,
                you would pass varia as the argument.
            wait (bool, optional): If `True`, the client will wait to send all tracked metadata to the server.
                This makes the call synchronous.
                Defaults to `None`.

        For more information, see the docs:
            https://docs.neptune.ai/api/field_types#delete_files
        """
        return self._pass_call_to_attr(function_name="delete_files", paths=paths, wait=wait)

    @check_protected_paths
    def download(
        self,
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        """Downloads the stored files to the working directory or to the specified destination.

        Available for the following field types:

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
            progress_bar: (bool or Type of progress bar, optional): progress bar to be used while downloading assets.
                If `None` or `True` the default tqdm-based progress bar will be used.
                If `False` no progress bar will be used.
                If a type of progress bar is passed, it will be used instead of the default one.
                Defaults to `None`.

        For more information, see the docs:
           https://docs.neptune.ai/api-reference/field-types
        """
        return self._pass_call_to_attr(function_name="download", destination=destination, progress_bar=progress_bar)

    def download_last(self, destination: str = None) -> None:
        """Downloads the stored files to the working directory or to the specified destination.

        Args:
            destination (str, optional): Path to where the file(s) should be downloaded.
                If `None` file will be downloaded to the working directory.
                If `destination` is a directory, the file will be downloaded to the specified directory with a filename
                composed from field name and extension (if present).
                If `destination` is a path to a file, the file will be downloaded under the specified name.
                Defaults to `None`.

        For more information, see the docs:
           https://docs.neptune.ai/api/field_types#download_last
        """
        return self._pass_call_to_attr(function_name="download_last", destination=destination)

    def fetch_hash(self) -> str:
        """Fetches the hash of an artifact.

        You may also want to check the docs:
           https://docs.neptune.ai/api/field_types#fetch_hash
        """
        return self._pass_call_to_attr(function_name="fetch_hash")

    def fetch_extension(self) -> str:
        """Fetches the extension of a file.

        You may also want to check the docs:
           https://docs.neptune.ai/api/field_types#fetch_extension
        """
        return self._pass_call_to_attr(function_name="fetch_extension")

    def fetch_files_list(self) -> List[ArtifactFileData]:
        """Fetches the list of files in an artifact and their metadata.

        You may also want to check the docs:
           https://docs.neptune.ai/api/field_types#fetch_files_list
        """
        return self._pass_call_to_attr(function_name="fetch_files_list")

    def list_fileset_files(self, path: Optional[str] = None) -> List[FileEntry]:
        """Fetches metadata of the file set.

        If the top-level artifact of the field is a directory, only the metadata of this directory is returned.
        You can use the `path` argument to list metadata of the files contained inside the directory or subdirectories.

        Args:
            path: Path to a nested directory, to get metadata of the files contained within the directory.

        Returns:
            List of FileEntry items with the following metadata: name, size (bytes), mtime (last modification time),
            and file type (file or directory).

        Examples:
            In this example, a Neptune run (RUN-100) has a FileSet field "dataset" containing a directory called "data",
            which has a subdirectory "samples" and a file "dataset.csv". The code for logging this would be:
            `run["dataset"].upload_files("data")`

            >>> import neptune
            >>> run = neptune.init_run(with_id="RUN-100")
            >>> run["dataset"].list_fileset_files()
            [FileEntry(name='data', size=None, mtime=datetime.datetime(2023, 8, 17, 10, 31, 54, 278601, tzinfo=tzutc()),
            file_type='directory')]
            >>> run["dataset"].list_fileset_files(path="data")
            [FileEntry(name='samples', size=None, mtime=datetime.datetime(2023, 8, 17, 10, 34, 6, 777017,
            tzinfo=tzutc()), file_type='directory'), FileEntry(name='dataset.csv', size=215,
            mtime=datetime.datetime(2023, 8, 17, 10, 31, 26, 402000, tzinfo=tzutc()), file_type='file')]
            >>> run["dataset"].list_fileset_files(path="data/samples")
            [FileEntry(name='sample_v2.csv', size=215, mtime=datetime.datetime(2023, 8, 17, 10, 31, 26, 491000,
            tzinfo=tzutc()), file_type='file'), FileEntry(name='sample_v3.csv', size=215, mtime=datetime.datetime(2023,
            8, 17, 10, 31, 26, 338000, tzinfo=tzutc()), file_type='file'), ...]

        For more information, see the API reference:
           https://docs.neptune.ai/api/field_types#list_fileset_files
        """
        return self._pass_call_to_attr(function_name="list_fileset_files", path=path)

    def _pass_call_to_attr(self, function_name, **kwargs):
        return getattr(self._get_attribute(), function_name)(**kwargs)

    @check_protected_paths
    def track_files(self, path: str, *, destination: str = None, wait: bool = False) -> None:
        """Creates an artifact tracking some files.

        You may also want to check the docs:
           https://docs.neptune.ai/api/field_types#track_files
        """
        with self._container.lock():
            attr = self._container.get_attribute(self._path)
            if attr is None:
                attr = Artifact(self._container, parse_path(self._path))
                self._container.set_attribute(self._path, attr)

            attr.track_files(path=path, destination=destination, wait=wait)

    def __delitem__(self, path) -> None:
        self.pop(path)


class ExtendUtils:
    @staticmethod
    def transform_to_extend_format(value):
        """Preserves the nested structure created by `Namespaces` and `dict_like` objects,
        but replaces all other values with single-element lists,
        so the work can be delegated to `extend` method."""
        if isinstance(value, Namespace) or is_dict_like(value):
            return {k: ExtendUtils.transform_to_extend_format(v) for k, v in value.items()}

        if isinstance(value, StringifyValue):
            return stringify_unsupported([value.value])

        return [value]

    @staticmethod
    def validate_values_for_extend(values, steps, timestamps):
        """Validates if the input data is a collection or a namespace with collections leafs.
        If steps or timestamps are passed, check if its length is equal to all given values."""
        collections_lengths = set(ExtendUtils.generate_leaf_collection_lengths(values))

        if len(collections_lengths) > 1:
            if steps is not None:
                raise NeptuneUserApiInputException("Number of steps must be equal to the number of values")
            if timestamps is not None:
                raise NeptuneUserApiInputException("Number of timestamps must be equal to the number of values")
        else:
            common_collections_length = next(iter(collections_lengths))
            if steps is not None and common_collections_length != len(steps):
                raise NeptuneUserApiInputException("Number of steps must be equal to the number of values")
            if timestamps is not None and common_collections_length != len(timestamps):
                raise NeptuneUserApiInputException("Number of timestamps must be equal to the number of values")

    @staticmethod
    def generate_leaf_collection_lengths(values) -> Iterator[int]:
        if is_stringify_value(values):
            values = values.value

        if isinstance(values, Namespace) or is_dict_like(values):
            for val in values.values():
                yield from ExtendUtils.generate_leaf_collection_lengths(val)
        elif is_collection(values):
            yield len(values)
        else:
            raise NeptuneUserApiInputException("Values must be a collection or namespace leafs must be collections")
