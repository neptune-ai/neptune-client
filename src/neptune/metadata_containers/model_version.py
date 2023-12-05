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
__all__ = ["ModelVersion"]

import os
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
)

from typing_extensions import Literal

from neptune.attributes.constants import (
    SYSTEM_NAME_ATTRIBUTE_PATH,
    SYSTEM_STAGE_ATTRIBUTE_PATH,
)
from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.exceptions import (
    InactiveModelVersionException,
    NeedExistingModelVersionForReadOnlyMode,
    NeptuneMissingRequiredInitParameter,
    NeptuneOfflineModeChangeStageException,
)
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.metadata_containers import MetadataContainer
from neptune.metadata_containers.abstract import NeptuneObjectCallback
from neptune.types.mode import Mode
from neptune.types.model_version_stage import ModelVersionStage

if TYPE_CHECKING:
    from neptune.internal.background_job import BackgroundJob


class ModelVersion(MetadataContainer):
    """Initializes a ModelVersion object from an existing or new model version.

    Before creating model versions, you must first register a model by creating a Model object.

    A ModelVersion object is suitable for storing model metadata that is version-specific. It does not track
    background metrics or logs automatically, but you can assign metadata to the model version just like you can
    for runs. You can use the parent Model object to store metadata that is common to all versions of the model.
    To learn more about model registry, see the docs: https://docs.neptune.ai/model_registry/overview/

    To manage the stage of a model version, use its `change_stage()` method or use the menu in the web app.

    You can also use the ModelVersion object as a context manager (see examples).

    Args:
        with_id: The Neptune identifier of an existing model version to resume, such as "CLS-PRE-3".
            The identifier is stored in the model version's "sys/id" field.
            If left empty, a new model version is created.
        name: Custom name for the model version. You can add it as a column in the model versions table
            ("sys/name"). You can also edit the name in the app, in the information view.
        model: Identifier of the model for which the new version should be created.
            Required when creating a new model version.
            You can find the model ID in the leftmost column of the models table, or in a model's "sys/id" field.
        project: Name of a project in the form `workspace-name/project-name`.
            If None, the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token.
            If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        mode: Connection mode in which the tracking will work.
            If None (default), the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, "async" is used by default.
            Possible values are `async`, `sync`, `read-only`, and `debug`.
        flush_period: In the asynchronous (default) connection mode, how often disk flushing is triggered
            (in seconds).
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.
            For more information about proxies, see the Requests documentation.
        async_lag_callback: Custom callback which is called if the lag between a queued operation and its
            synchronization with the server exceeds the duration defined by `async_lag_threshold`. The callback
            should take a ModelVersion object as the argument and can contain any custom code, such as calling
            `stop()` on the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK` environment variable to `TRUE`.
        async_lag_threshold: In seconds, duration between the queueing and synchronization of an operation.
            If a lag callback (default callback enabled via environment variable or custom callback passed to the
            `async_lag_callback` argument) is enabled, the callback is called when this duration is exceeded.
        async_no_progress_callback: Custom callback which is called if there has been no synchronization progress
            whatsoever for the duration defined by `async_no_progress_threshold`. The callback should take a
            ModelVersion object as the argument and can contain any custom code, such as calling `stop()` on the
            object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK` environment variable to `TRUE`.
        async_no_progress_threshold: In seconds, for how long there has been no synchronization progress since the
            object was initialized. If a no-progress callback (default callback enabled via environment variable or
            custom callback passed to the `async_no_progress_callback` argument) is enabled, the callback is called
            when this duration is exceeded.

    Returns:
        ModelVersion object that is used to manage the model version and log metadata to it.

    Examples:

        >>> import neptune

        Creating a new model version:

        >>> # Create a new model version for a model with identifier "CLS-PRE"
        ... model_version = neptune.init_model_version(model="CLS-PRE")
        >>> model_version["your/structure"] = some_metadata

        >>> # You can provide the project parameter as an environment variable
        ... # or directly in the init_model_version() function:
        ... model_version = neptune.init_model_version(
        ...    model="CLS-PRE",
        ...    project="ml-team/classification",
        ... )

        >>> # Or initialize with the constructor:
        ... model_version = ModelVersion(model="CLS-PRE")

        Connecting to an existing model version:

        >>> # Initialize an existing model version with identifier "CLS-PRE-12"
        ... model_version = neptune.init_model_version(with_id="CLS-PRE-12")

        >>> # To prevent modifications when connecting to an existing model version,
        ... # you can connect in read-only mode:
        ... model_version = neptune.init_model(with_id="CLS-PRE-12", mode="read-only")

        Using the ModelVersion object as context manager:

        >>> with ModelVersion(model="CLS-PRE") as model_version:
        ...     model_version["metadata"] = some_metadata

    For more, see the docs:
        Initializing a model version:
            https://docs.neptune.ai/api/neptune#init_model_version
        ModelVersion class reference:
            https://docs.neptune.ai/api/model_version/
    """

    container_type = ContainerType.MODEL_VERSION

    def __init__(
        self,
        with_id: Optional[str] = None,
        *,
        name: Optional[str] = None,
        model: Optional[str] = None,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Optional[Literal["async", "sync", "read-only", "debug"]] = None,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
        async_lag_callback: Optional[NeptuneObjectCallback] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[NeptuneObjectCallback] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
    ) -> None:
        verify_type("with_id", with_id, (str, type(None)))
        verify_type("name", name, (str, type(None)))
        verify_type("model", model, (str, type(None)))
        verify_type("project", project, (str, type(None)))
        verify_type("mode", mode, (str, type(None)))

        self._model: Optional[str] = model
        self._with_id: Optional[str] = with_id
        self._name: Optional[str] = DEFAULT_NAME if model is None and name is None else name

        # make mode proper Enum instead of string
        mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

        if mode == Mode.OFFLINE:
            raise NeptuneException("ModelVersion can't be initialized in OFFLINE mode")

        if mode == Mode.DEBUG:
            project = OFFLINE_PROJECT_QUALIFIED_NAME

        super().__init__(
            project=project,
            api_token=api_token,
            mode=mode,
            flush_period=flush_period,
            proxies=proxies,
            async_lag_callback=async_lag_callback,
            async_lag_threshold=async_lag_threshold,
            async_no_progress_callback=async_no_progress_callback,
            async_no_progress_threshold=async_no_progress_threshold,
        )

    def _get_or_create_api_object(self) -> ApiExperiment:
        project_workspace = self._project_api_object.workspace
        project_name = self._project_api_object.name
        project_qualified_name = f"{project_workspace}/{project_name}"

        if self._with_id is not None:
            # with_id (resume existing model_version) has priority over model (creating a new model_version)
            return self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._with_id),
                expected_container_type=self.container_type,
            )
        elif self._model is not None:
            if self._mode == Mode.READ_ONLY:
                raise NeedExistingModelVersionForReadOnlyMode()

            api_model = self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._model),
                expected_container_type=ContainerType.MODEL,
            )
            return self._backend.create_model_version(project_id=self._project_api_object.id, model_id=api_model.id)
        else:
            raise NeptuneMissingRequiredInitParameter(
                parameter_name="model",
                called_function="init_model_version",
            )

    def _get_background_jobs(self) -> List["BackgroundJob"]:
        return [PingBackgroundJob()]

    def _write_initial_attributes(self):
        if self._name is not None:
            self[SYSTEM_NAME_ATTRIBUTE_PATH] = self._name

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveModelVersionException(label=self._sys_id)

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_model_version_url(
            model_version_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
            model_id=self["sys/model_id"].fetch(),
        )

    def change_stage(self, stage: str) -> None:
        """Changes the stage of the model version.

        This method is always synchronous, which means that Neptune will wait for all other calls to reach the Neptune
            servers before executing it.
        Args:
            stage: The new stage of the model version.
                Possible values are `none`, `staging`, `production`, and `archived`.
        Examples:
            >>> import neptune
            >>> model_version = neptune.init_model_version(with_id="CLS-TREE-3")
            >>> # If the model is good enough, promote it to the staging
            ... val_acc = model_version["validation/metrics/acc"].fetch()
            >>> if val_acc >= ACC_THRESHOLD:
            ...     model_version.change_stage("staging")

        Learn more about stage management in the docs:
            https://docs.neptune.ai/model_registry/managing_stage/
        API reference:
            https://docs.neptune.ai/api/model_version/#change_stage
        """
        mapped_stage = ModelVersionStage(stage)

        if isinstance(self._op_processor, OfflineOperationProcessor):
            raise NeptuneOfflineModeChangeStageException()

        self.wait()

        with self.lock():
            attr = self.get_attribute(SYSTEM_STAGE_ATTRIBUTE_PATH)
            # We are sure that such attribute exists, because
            # SYSTEM_STAGE_ATTRIBUTE_PATH is set by default on ModelVersion creation
            assert attr is not None, f"No {SYSTEM_STAGE_ATTRIBUTE_PATH} found in model version"
            attr.process_assignment(
                value=mapped_stage.value,
                wait=True,
            )
