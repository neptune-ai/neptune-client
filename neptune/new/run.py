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
import atexit
import threading
import time
import traceback
import uuid
from contextlib import AbstractContextManager
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Dict, List, Optional, Union

import click

from neptune.exceptions import UNIX_STYLES
from neptune.new.attributes import create_attribute_from_type
from neptune.new.attributes.attribute import Attribute
from neptune.new.attributes.namespace import NamespaceBuilder, Namespace as NamespaceAttr
from neptune.new.exceptions import (
    MetadataInconsistency, InactiveRunException, NeptunePossibleLegacyUsageException,
)
from neptune.new.handler import Handler
from neptune.new.internal.backends.api_model import AttributeType
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.operation import DeleteAttribute
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.run_structure import RunStructure
from neptune.new.internal.utils import (
    is_bool, is_float, is_float_like, is_int, is_string, is_string_like, verify_type, is_dict_like,
)
from neptune.new.internal.utils.paths import parse_path
from neptune.new.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.new.types import Boolean, Integer
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.namespace import Namespace
from neptune.new.types.value import Value


class RunState(Enum):
    CREATED = 'created'
    STARTED = 'started'
    STOPPING = 'stopping'
    STOPPED = 'stopped'


def assure_run_not_stopped(fun):
    @wraps(fun)
    def inner_fun(self, *args, **kwargs):
        # pylint: disable=protected-access
        if self._state == RunState.STOPPED:
            raise InactiveRunException(short_id=self._short_id)
        return fun(self, *args, **kwargs)

    return inner_fun


LEGACY_METHODS = (
    'create_experiment',
    'send_metric', 'log_metric',
    'send_text', 'log_text',
    'send_image', 'log_image',
    'send_artifact', 'log_artifact', 'delete_artifacts',
    'download_artifact', 'download_sources', 'download_artifacts',
    'reset_log', 'get_parameters',
    'get_properties', 'set_property', 'remove_property',
    'get_hardware_utilization', 'get_numeric_channels_values',
)


class Run(AbstractContextManager):
    """A Run in Neptune is a representation of all metadata that you log to Neptune.

    Beginning when you start a tracked run with `neptune.init()` and ending when the script finishes
    or when you explicitly stop the experiment with `.stop()`.

    You can log many ML metadata types, including:
        * metrics
        * losses
        * model weights
        * images
        * interactive charts
        * predictions
        * and much more

    Examples:
        >>> import neptune.new as neptune

        >>> # Create new experiment
        ... run = neptune.init('my_workspace/my_project')

        >>> # Log parameters
        ... params = {'max_epochs': 10, 'optimizer': 'Adam'}
        ... run['parameters'] = params

        >>> # Log metadata
        ... run['train/metric_name'].log()
        >>> run['predictions'].log(image)
        >>> run['model'].upload(path_to_model)

        >>> # Log whatever else you want
        ... pass

        >>> # Stop tracking and clean up
        ... run.stop()

    You may also want to check `Run docs page`_.

    .. _Run docs page:
       https://docs.neptune.ai/api-reference/run
    """

    last_run = None  # "static" instance of recently created Run

    def __init__(
            self,
            _uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            background_job: BackgroundJob,
            workspace: str,
            project_name: str,
            short_id: str,
            monitoring_namespace: str = "monitoring",
    ):
        self._uuid = _uuid
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure: RunStructure[Attribute, NamespaceAttr] = RunStructure(NamespaceBuilder(self))
        self._lock = threading.RLock()
        self._state = RunState.CREATED
        self._workspace = workspace
        self._project_name = project_name
        self._short_id = short_id
        self.monitoring_namespace = monitoring_namespace

        Run.last_run = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()

    def __getattr__(self, item):
        if item in LEGACY_METHODS:
            raise NeptunePossibleLegacyUsageException()
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    @assure_run_not_stopped
    def __getitem__(self, path: str) -> 'Handler':
        return Handler(self, path)

    @assure_run_not_stopped
    def __setitem__(self, key: str, value) -> None:
        self.__getitem__(key).assign(value)

    @assure_run_not_stopped
    def __delitem__(self, path) -> None:
        self.pop(path)

    @assure_run_not_stopped
    def assign(self, value, wait: bool = False) -> None:
        """Assign values to multiple fields from a dictionary.
        You can use this method to quickly log all run's parameters.

        Args:
            value (dict): A dictionary with values to assign, where keys become the paths of the fields.
                The dictionary can be nested - in such case the path will be a combination of all keys.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `True`.

        Examples:
            >>> import neptune.new as neptune
            >>> run = neptune.init()

            >>> # Assign multiple fields from a dictionary
            ... params = {"max_epochs": 10, "optimizer": "Adam"}
            >>> run["parameters"] = params

            >>> # You can always log explicitely parameters one by one
            ... run["parameters/max_epochs"] = 10
            >>> run["parameters/optimizer"] = "Adam"

            >>> # Dictionaries can be nested
            ... params = {"train": {"max_epochs": 10}}
            >>> run["parameters"] = params
            >>> # This will log 10 under path "parameters/train/max_epochs"

        You may also want to check `assign docs page`_.

        .. _assign docs page:
            https://docs.neptune.ai/api-reference/run#assign
        """
        self._get_root_handler().assign(value, wait)

    @assure_run_not_stopped
    def fetch(self) -> dict:
        """Fetch values of all non-File Atom fields as a dictionary.
        The result will preserve the hierarchical structure of the run's metadata, but will contain only non-File Atom
        fields.
        You can use this method to quickly retrieve previous run's parameters.

        Returns:
            `dict` containing all non-File Atom fields values.

        Examples:
            >>> import neptune.new as neptune
            >>> resumed_run = neptune.init(run="HEL-3")
            >>> params = resumed_run['model/parameters'].fetch()

            >>> run_data = resumed_run.fetch()

            >>> print(run_data)
            >>> # this will print out all Atom attributes stored in run as a dict

        You may also want to check `fetch docs page`_.

        .. _fetch docs page:
            https://docs.neptune.ai/api-reference/run#fetch
        """
        return self._get_root_handler().fetch()

    def ping(self):
        self._backend.ping_run(self._uuid)

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._state = RunState.STARTED

    def stop(self, seconds: Optional[Union[float, int]] = None) -> None:
        """Stops the tracked run and kills the synchronization thread.

        `.stop()` will be automatically called when a script that created the run finishes or on the destruction
        of Neptune context.

        When using Neptune with Jupyter notebooks it's a good practice to stop the tracked run manually as it
        will be stopped automatically only when the Jupyter kernel stops.

        Args:
            seconds (int or float, optional): Seconds to wait for all tracking calls to finish
                before stopping the tracked run.
                If `None` will wait for all tracking calls to finish. Defaults to `True`.

        Examples:
            If you are creating tracked runs from the script you don't need to call `.stop()`:

            >>> import neptune.new as neptune
            >>> run = neptune.init()

            >>> # Your training or monitoring code
            ... pass
            ... # If you are executing Python script .stop()
            ... # is automatically called at the end for every run

            If you are performing multiple training jobs from one script one after the other it is a good practice
            to `.stop()` the finished tracked runs as every open run keeps an open connection with Neptune,
            monitors hardware usage, etc. You can also use Context Managers - Neptune will automatically call `.stop()`
            on the destruction of Run context:

            >>> import neptune.new as neptune

            >>> # If you are running consecutive training jobs from the same script
            ... # stop the tracked runs manually at the end of single training job
            ... for config in configs:
            ...   run = neptune.init()
            ...   # Your training or monitoring code
            ...   pass
            ...   run.stop()

            >>> # You can also use with statement and context manager
            ... for config in configs:
            ...   with neptune.init() as run:
            ...     # Your training or monitoring code
            ...     pass
            ...     # .stop() is automatically called
            ...     # when code execution exits the with statement

        .. warning::
            If you are using Jupyter notebooks for creating your runs you need to manually invoke `.stop()` once the
            training and evaluation is done.

        You may also want to check `stop docs page`_.

        .. _stop docs page:
            https://docs.neptune.ai/api-reference/run#stop
        """
        verify_type("seconds", seconds, (float, int, type(None)))
        if self._state != RunState.STARTED:
            return

        self._state = RunState.STOPPING
        ts = time.time()
        click.echo("Shutting down background jobs, please wait a moment...")
        self._bg_job.stop()
        self._bg_job.join(seconds)
        click.echo("Done!")
        with self._lock:
            sec_left = None if seconds is None else seconds - (time.time() - ts)
            self._op_processor.stop(sec_left)
        self._state = RunState.STOPPED

    def get_structure(self) -> Dict[str, Any]:
        """Returns a run's metadata structure in form of a dictionary.

        This method can be used to traverse the run's metadata structure programmatically when using Neptune in
        automated workflows.

        .. danger::
            The returned object is a deep copy of an internal run's structure.

        Returns:
            ``dict``: with the run's metadata structure.

        """

        # This is very weird pylint false-positive.
        # pylint: disable=no-member
        return self._structure.get_structure().to_dict()

    def print_structure(self) -> None:
        """Pretty prints the structure of the run's metadata.

        Paths are ordered lexicographically and the whole structure is neatly colored.
        """
        self._print_structure_impl(self.get_structure(), indent=0)

    def get_run_url(self) -> str:
        """Returns the URL the run can be accessed with in the browser
        """
        return self._backend.get_run_url(self._uuid, self._workspace, self._project_name, self._short_id)

    def _print_structure_impl(self, struct: dict, indent: int) -> None:
        for key in sorted(struct.keys()):
            click.echo("    " * indent, nl=False)
            if isinstance(struct[key], dict):
                click.echo("{blue}'{key}'{end}:".format(
                    blue=UNIX_STYLES['blue'],
                    key=key,
                    end=UNIX_STYLES['end']))
                self._print_structure_impl(struct[key], indent=indent + 1)
            else:
                click.echo("{blue}'{key}'{end}: {type}".format(
                    blue=UNIX_STYLES['blue'],
                    key=key,
                    end=UNIX_STYLES['end'],
                    type=type(struct[key]).__name__))

    def define(self,
               path: str,
               value: Union[Value, int, float, str, datetime],
               wait: bool = False
               ) -> Attribute:
        if isinstance(value, Value):
            pass
        elif is_bool(value):
            value = Boolean(value)
        elif is_int(value):
            value = Integer(value)
        elif is_float(value):
            value = Float(value)
        elif is_string(value):
            value = String(value)
        elif isinstance(value, datetime):
            value = Datetime(value)
        elif is_float_like(value):
            value = Float(float(value))
        elif is_dict_like(value):
            value = Namespace(value)
        elif is_string_like(value):
            value = String(str(value))
        else:
            raise TypeError("Value of unsupported type {}".format(type(value)))
        parsed_path = parse_path(path)

        with self._lock:
            old_attr = self._structure.get(parsed_path)
            if old_attr:
                raise MetadataInconsistency("Attribute or namespace {} is already defined".format(path))
            attr = ValueToAttributeVisitor(self, parsed_path).visit(value)
            attr.assign(value, wait)
            self._structure.set(parsed_path, attr)
            return attr

    def get_attribute(self, path: str) -> Optional[Attribute]:
        with self._lock:
            return self._structure.get(parse_path(path))

    def set_attribute(self, path: str, attribute: Attribute) -> Optional[Attribute]:
        with self._lock:
            return self._structure.set(parse_path(path), attribute)

    def exists(self, path: str) -> bool:
        verify_type("path", path, str)
        return self.get_attribute(path) is not None

    def pop(self, path: str, wait: bool = False) -> None:
        """Removes the field stored under the path completely and all data associated with it.

        Args:
            path (str): Path of the field to be removed.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `True`.

        Examples:
            >>> import neptune.new as neptune
            >>> run = neptune.init()

            >>> run['parameters/learninggg_rata'] = 0.3

            >>> # Delete a field along with it's data
            ... run.pop('parameters/learninggg_rata')

            >>> run['parameters/learning_rate'] = 0.3

            >>> # Training finished
            ... run['trained_model'].upload('model.pt')
            >>> # 'model_checkpoint' is a File field
            ... run.pop('model_checkpoint')

        You may also want to check `pop docs page`_.

        .. _pop docs page:
           https://docs.neptune.ai/api-reference/run#pop
        """
        verify_type("path", path, str)
        with self._lock:
            self._pop_impl(parse_path(path), wait)

    def _pop_impl(self, parsed_path: List[str], wait: bool):
        attribute = self._structure.get(parsed_path)
        if isinstance(attribute, NamespaceAttr):
            self._pop_namespace(attribute, wait)
        else:
            self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait)
            self._structure.pop(parsed_path)

    def _pop_namespace(self, namespace: NamespaceAttr, wait: bool):
        children = list(namespace)
        for key in children:
            sub_attr_path = namespace._path + [key] # pylint: disable=protected-access
            self._pop_impl(sub_attr_path, wait)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self, disk_only=False) -> None:
        """Wait for all the tracking calls to finish.

        Args:
            disk_only (bool, optional, default is False): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `False`.

        You may also want to check `wait docs page`_.

        .. _wait docs page:
            https://docs.neptune.ai/api-reference/run#wait
        """
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

    def sync(self, wait: bool = True) -> None:
        """Synchronizes local representation of the run with Neptune servers.

        Args:
            wait (bool, optional, default is False): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `True`.

        Examples:
            >>> import neptune.new as neptune

            >>> # Connect to a run from Worker #3
            ... worker_id = 3
            >>> run = neptune.init(run='DIST-43', monitoring_namespace='monitoring/{}'.format(worker_id))

            >>> # Try to access logs that were created in meantime by Worker #2
            ... worker_2_status = run['status/2'].fetch() # Error if this field was created after this script starts

            >>> run.sync() # Synchronizes local representation with Neptune servers.
            >>> worker_2_status = run['status/2'].fetch() # No error

        You may also want to check `sync docs page`_.

        .. _sync docs page:
            https://docs.neptune.ai/api-reference/run#sync
        """
        with self._lock:
            if wait:
                self._op_processor.wait()
            attributes = self._backend.get_attributes(self._uuid)
            self._structure.clear()
            for attribute in attributes:
                self._define_attribute(parse_path(attribute.path), attribute.type)

    def _define_attribute(self, _path: List[str], _type: AttributeType):
        attr = create_attribute_from_type(_type, self, _path)
        self._structure.set(_path, attr)

    def _get_root_handler(self):
        return Handler(self, "")

    def _shutdown_hook(self):
        self.stop()
