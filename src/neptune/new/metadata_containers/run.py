#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["Run"]

import threading
from typing import (
    Any,
    Dict,
    Optional,
    Union,
)

from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import (
    SysId,
    UniqueId,
)
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.utils.deprecation import deprecated
from neptune.new.metadata_containers import MetadataContainer
from neptune.new.types.mode import Mode


class Run(MetadataContainer):
    """A Run in Neptune is a representation of all metadata that you log to Neptune.

    Beginning when you start a tracked run with `neptune.init_run()` and ending when the script finishes
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
        ... run = neptune.init_run('my_workspace/my_project')

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
       https://docs.neptune.ai/api/run
    """

    last_run = None  # "static" instance of recently created Run

    container_type = ContainerType.RUN

    LEGACY_METHODS = (
        "create_experiment",
        "send_metric",
        "log_metric",
        "send_text",
        "log_text",
        "send_image",
        "log_image",
        "send_artifact",
        "log_artifact",
        "delete_artifacts",
        "download_artifact",
        "download_sources",
        "download_artifacts",
        "reset_log",
        "get_parameters",
        "get_properties",
        "set_property",
        "remove_property",
        "get_hardware_utilization",
        "get_numeric_channels_values",
    )

    def __init__(
        self,
        *,
        id_: UniqueId,
        mode: Mode,
        backend: NeptuneBackend,
        op_processor: OperationProcessor,
        background_job: BackgroundJob,
        lock: threading.RLock,
        workspace: str,
        project_name: str,
        sys_id: SysId,
        project_id: UniqueId,
        monitoring_namespace: str = "monitoring",
    ):
        super().__init__(
            id_=id_,
            mode=mode,
            backend=backend,
            op_processor=op_processor,
            background_job=background_job,
            lock=lock,
            project_id=project_id,
            project_name=project_name,
            workspace=workspace,
            sys_id=sys_id,
        )
        self.monitoring_namespace = monitoring_namespace

        Run.last_run = self

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api/run#stop"

    @property
    def _label(self) -> str:
        return self._sys_id

    @deprecated(alternative="get_url")
    def get_run_url(self) -> str:
        """Returns the URL the run can be accessed with in the browser"""
        return self._url

    @property
    def _url(self) -> str:
        return self._backend.get_run_url(
            run_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
        )

    @property
    def _metadata_url(self) -> str:
        return self._url

    @property
    def _short_id(self) -> str:
        return self._sys_id

    def assign(self, value, wait: bool = False) -> None:
        """Assign values to multiple fields from a dictionary.
        You can use this method to quickly log all run's parameters.
        Args:
            value (dict): A dictionary with values to assign, where keys become the paths of the fields.
                The dictionary can be nested - in such case the path will be a combination of all keys.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `False`.
        Examples:
            >>> import neptune.new as neptune
            >>> run = neptune.init_run()
            >>> # Assign multiple fields from a dictionary
            ... params = {"max_epochs": 10, "optimizer": "Adam"}
            >>> run["parameters"] = params
            >>> # You can always log explicitly parameters one by one
            ... run["parameters/max_epochs"] = 10
            >>> run["parameters/optimizer"] = "Adam"
            >>> # Dictionaries can be nested
            ... params = {"train": {"max_epochs": 10}}
            >>> run["parameters"] = params
            >>> # This will log 10 under path "parameters/train/max_epochs"
        You may also want to check `assign docs page`_.
        .. _assign docs page:
            https://docs.neptune.ai/api/run#assign
        """
        return MetadataContainer.assign(self, value=value, wait=wait)

    def fetch(self) -> dict:
        """Fetch values of all non-File Atom fields as a dictionary.
        The result will preserve the hierarchical structure of the run's metadata, but will contain only non-File Atom
        fields.
        You can use this method to quickly retrieve previous run's parameters.
        Returns:
            `dict` containing all non-File Atom fields values.
        Examples:
            >>> import neptune.new as neptune
            >>> resumed_run = neptune.init_run(with_id="HEL-3")
            >>> params = resumed_run['model/parameters'].fetch()
            >>> run_data = resumed_run.fetch()
            >>> print(run_data)
            >>> # this will print out all Atom attributes stored in run as a dict
        You may also want to check `fetch docs page`_.
        .. _fetch docs page:
            https://docs.neptune.ai/api/run#fetch
        """
        return MetadataContainer.fetch(self)

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
            >>> run = neptune.init_run()
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
            ...   run = neptune.init_run()
            ...   # Your training or monitoring code
            ...   pass
            ...   run.stop()
            >>> # You can also use with statement and context manager
            ... for config in configs:
            ...   with neptune.init_run() as run:
            ...     # Your training or monitoring code
            ...     pass
            ...     # .stop() is automatically called
            ...     # when code execution exits the with statement
        .. warning::
            If you are using Jupyter notebooks for creating your runs you need to manually invoke `.stop()` once the
            training and evaluation is done.
        You may also want to check `stop docs page`_.
        .. _stop docs page:
            https://docs.neptune.ai/api/run#stop
        """
        return MetadataContainer.stop(self, seconds=seconds)

    def get_structure(self) -> Dict[str, Any]:
        """Returns a run's metadata structure in form of a dictionary.
        This method can be used to traverse the run's metadata structure programmatically
        when using Neptune in automated workflows.
        .. danger::
            The returned object is a deep copy of an internal run's structure.
        Returns:
            ``dict``: with the run's metadata structure.
        """
        return MetadataContainer.get_structure(self)

    def print_structure(self) -> None:
        """Pretty prints the structure of the run's metadata.
        Paths are ordered lexicographically and the whole structure is neatly colored.
        """
        return MetadataContainer.print_structure(self)

    def pop(self, path: str, wait: bool = False) -> None:
        """Removes the field stored under the path completely and all data associated with it.
        Args:
            path (str): Path of the field to be removed.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `True`.
        Examples:
            >>> import neptune.new as neptune
            >>> run = neptune.init_run()
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
           https://docs.neptune.ai/api/run#pop
        """
        return MetadataContainer.pop(self, path=path, wait=wait)

    def wait(self, disk_only=False) -> None:
        """Wait for all the tracking calls to finish.
        Args:
            disk_only (bool, optional, default is False): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `False`.
        You may also want to check `wait docs page`_.
        .. _wait docs page:
            https://docs.neptune.ai/api/run#wait
        """
        return MetadataContainer.wait(self, disk_only=disk_only)

    def sync(self, wait: bool = True) -> None:
        """Synchronizes local representation of the run with Neptune servers.
        Args:
            wait (bool, optional, default is True): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `True`.
        Examples:
            >>> import neptune.new as neptune
            >>> # Connect to a run from Worker #3
            ... worker_id = 3
            >>> run = neptune.init_run(with_id='DIST-43', monitoring_namespace='monitoring/{}'.format(worker_id))
            >>> # Try to access logs that were created in meantime by Worker #2
            ... worker_2_status = run['status/2'].fetch() # Error if this field was created after this script starts
            >>> run.sync() # Synchronizes local representation with Neptune servers.
            >>> worker_2_status = run['status/2'].fetch() # No error
        You may also want to check `sync docs page`_.
        .. _sync docs page:
            https://docs.neptune.ai/api/run#sync
        """
        return MetadataContainer.sync(self, wait=wait)
