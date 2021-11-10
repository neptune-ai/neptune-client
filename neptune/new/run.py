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
import threading

from neptune.new.attribute_container import AttributeContainer
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.operation_processors.operation_processor import (
    OperationProcessor,
)
from neptune.new.internal.state import ContainerState

RunState = ContainerState  # backwards compatibility


class Run(AttributeContainer):
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
        _id: str,
        backend: NeptuneBackend,
        op_processor: OperationProcessor,
        background_job: BackgroundJob,
        lock: threading.RLock,
        workspace: str,
        project_name: str,
        short_id: str,
        project_id: str,
        monitoring_namespace: str = "monitoring",
    ):
        super().__init__(_id, backend, op_processor, background_job, lock, project_id)
        self._workspace = workspace
        self._project_name = project_name
        self._short_id = short_id
        self.monitoring_namespace = monitoring_namespace

        Run.last_run = self

    def get_run_url(self) -> str:
        """Returns the URL the run can be accessed with in the browser"""
        return self._backend.get_run_url(
            self._id, self._workspace, self._project_name, self._short_id
        )
