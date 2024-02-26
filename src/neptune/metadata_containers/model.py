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
__all__ = ["Model"]

import os
from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Optional,
)

from typing_extensions import Literal

from neptune.attributes.constants import SYSTEM_NAME_ATTRIBUTE_PATH
from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.exceptions import (
    InactiveModelException,
    NeedExistingModelForReadOnlyMode,
    NeptuneMissingRequiredInitParameter,
    NeptuneModelKeyAlreadyExistsError,
    NeptuneObjectCreationConflict,
)
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQueryAggregate,
    NQLQueryAttribute,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.metadata_containers import MetadataContainer
from neptune.metadata_containers.abstract import NeptuneObjectCallback
from neptune.table import Table
from neptune.types.mode import Mode
from neptune.typing import (
    ProgressBarCallback,
    ProgressBarType,
)

if TYPE_CHECKING:
    from neptune.internal.background_job import BackgroundJob


class Model(MetadataContainer):
    """Initializes a Model object from an existing or new model.

    You can use this to create a new model from code or to perform actions on existing models.

    A Model object is suitable for storing model metadata that is common to all versions (you can use ModelVersion
    objects to track version-specific metadata). It does not track background metrics or logs automatically,
    but you can assign metadata to the Model object just like you can for runs.
    To learn more about model registry, see the docs: https://docs.neptune.ai/model_registry/overview/

    You can also use the Model object as a context manager (see examples).

    Args:
         with_id: The Neptune identifier of an existing model to resume, such as "CLS-PRE".
            The identifier is stored in the model's "sys/id" field.
            If left empty, a new model is created.
        name: Custom name for the model. You can add it as a column in the models table ("sys/name").
            You can also edit the name in the app, in the information view.
        key: Key for the model. Required when creating a new model.
            Used together with the project key to form the model identifier.
            Must be uppercase and unique within the project.
        project: Name of a project in the form `workspace-name/project-name`.
            If None, the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token.
            If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        mode: Connection mode in which the tracking will work.
            If `None` (default), the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, "async" is used by default.
            Possible values are `async`, `sync`, `read-only`, and `debug`.
        flush_period: In the asynchronous (default) connection mode, how often disk flushing is triggered
            (in seconds).
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.
            For more information about proxies, see the Requests documentation.
        async_lag_callback: Custom callback which is called if the lag between a queued operation and its
            synchronization with the server exceeds the duration defined by `async_lag_threshold`. The callback
            should take a Model object as the argument and can contain any custom code, such as calling `stop()` on
            the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK` environment variable to `TRUE`.
        async_lag_threshold: In seconds, duration between the queueing and synchronization of an operation.
            If a lag callback (default callback enabled via environment variable or custom callback passed to the
            `async_lag_callback` argument) is enabled, the callback is called when this duration is exceeded.
        async_no_progress_callback: Custom callback which is called if there has been no synchronization progress
            whatsoever for the duration defined by `async_no_progress_threshold`. The callback should take a Model
            object as the argument and can contain any custom code, such as calling `stop()` on the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK` environment variable to `TRUE`.
        async_no_progress_threshold: In seconds, for how long there has been no synchronization progress since the
            object was initialized. If a no-progress callback (default callback enabled via environment variable or
            custom callback passed to the `async_no_progress_callback` argument) is enabled, the callback is called
            when this duration is exceeded.

    Returns:
        Model object that is used to manage the model and log metadata to it.

    Examples:

        >>> import neptune

        Creating a new model:

        >>> model = neptune.init_model(key="PRE")
        >>> model["metadata"] = some_metadata

        >>> # Or initialize with the constructor
        ... model = Model(key="PRE")

        >>> # You can provide the project parameter as an environment variable
        ... # or as an argument to the init_model() function:
        ... model = neptune.init_model(key="PRE", project="workspace-name/project-name")

        >>> # When creating a model, you can give it a name:
        ... model = neptune.init_model(key="PRE", name="Pre-trained model")

        Connecting to an existing model:

        >>> # Initialize existing model with identifier "CLS-PRE"
        ... model = neptune.init_model(with_id="CLS-PRE")

        >>> # To prevent modifications when connecting to an existing model, you can connect in read-only mode
        ... model = neptune.init_model(with_id="CLS-PRE", mode="read-only")

        Using the Model object as context manager:

        >>> with Model(key="PRE") as model:
        ...     model["metadata"] = some_metadata

    For details, see the docs:
        Initializing a model:
            https://docs.neptune.ai/api/neptune#init_model
        Model class reference:
            https://docs.neptune.ai/api/model
    """

    container_type = ContainerType.MODEL

    def __init__(
        self,
        with_id: Optional[str] = None,
        *,
        name: Optional[str] = None,
        key: Optional[str] = None,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Optional[Literal["async", "sync", "read-only", "debug"]] = None,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
        async_lag_callback: Optional[NeptuneObjectCallback] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[NeptuneObjectCallback] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
    ):
        verify_type("with_id", with_id, (str, type(None)))
        verify_type("name", name, (str, type(None)))
        verify_type("key", key, (str, type(None)))
        verify_type("project", project, (str, type(None)))
        verify_type("mode", mode, (str, type(None)))

        self._key: Optional[str] = key
        self._with_id: Optional[str] = with_id
        self._name: Optional[str] = DEFAULT_NAME if with_id is None and name is None else name

        # make mode proper Enum instead of string
        mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

        if mode == Mode.OFFLINE:
            raise NeptuneException("Model can't be initialized in OFFLINE mode")

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
            # with_id (resume existing model) has priority over key (creating a new model)
            #  additional creation parameters (e.g. name) are simply ignored in this scenario
            return self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._with_id),
                expected_container_type=self.container_type,
            )
        elif self._key is not None:
            if self._mode == Mode.READ_ONLY:
                raise NeedExistingModelForReadOnlyMode()

            try:
                return self._backend.create_model(project_id=self._project_api_object.id, key=self._key)
            except NeptuneObjectCreationConflict as e:
                base_url = self._backend.get_display_address()
                raise NeptuneModelKeyAlreadyExistsError(
                    model_key=self._key,
                    models_tab_url=f"{base_url}/{project_workspace}/{project_name}/models",
                ) from e
        else:
            raise NeptuneMissingRequiredInitParameter(
                parameter_name="key",
                called_function="init_model",
            )

    def _get_background_jobs(self) -> List["BackgroundJob"]:
        return [PingBackgroundJob()]

    def _write_initial_attributes(self):
        if self._name is not None:
            self[SYSTEM_NAME_ATTRIBUTE_PATH] = self._name

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveModelException(label=self._sys_id)

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_model_url(
            model_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
        )

    def fetch_model_versions_table(
        self,
        *,
        columns: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> Table:
        """Retrieve all versions of the given model.

        Args:
            columns: Names of columns to include in the table, as a list of field names.
                The Neptune ID ("sys/id") is included automatically.
                If `None` (default), all the columns of the model versions table are included.
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.

        Returns:
            `Table` object containing `ModelVersion` objects that match the specified criteria.

            Use `to_pandas()` to convert it to a pandas DataFrame.

        Examples:
            >>> import neptune
            ... # Initialize model with the ID "CLS-FOREST"
            ... model = neptune.init_model(with_id="CLS-FOREST")
            ... # Fetch the metadata of all the model's versions as a pandas DataFrame
            ... model_versions_df = model.fetch_model_versions_table().to_pandas()

            >>> # Include only the fields "params/lr" and "val/loss" as columns:
            ... model_versions_df = model.fetch_model_versions_table(columns=["params/lr", "val/loss"]).to_pandas()

            >>> # Sort model versions by size (space they take up in Neptune)
            ... model_versions_df = model.fetch_model_versions_table(sort_by="sys/size").to_pandas()
            ... # Extract the ID of the largest model version object
            ... largest_model_version_id = model_versions_df["sys/id"].values[0]

        See also the API referene:
            https://docs.neptune.ai/api/model/#fetch_model_versions_table
        """
        verify_type("limit", limit, (int, type(None)))
        verify_type("sort_by", sort_by, str)
        verify_type("ascending", ascending, bool)
        verify_type("progress_bar", progress_bar, (type(None), bool, type(ProgressBarCallback)))

        if isinstance(limit, int) and limit <= 0:
            raise ValueError(f"Parameter 'limit' must be a positive integer or None. Got {limit}.")
        return MetadataContainer._fetch_entries(
            self,
            child_type=ContainerType.MODEL_VERSION,
            query=NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/model_id",
                        value=self._sys_id,
                        operator=NQLAttributeOperator.EQUALS,
                        type=NQLAttributeType.STRING,
                    ),
                    NQLQueryAttribute(
                        name="sys/trashed",
                        type=NQLAttributeType.BOOLEAN,
                        operator=NQLAttributeOperator.EQUALS,
                        value=False,
                    ),
                ],
                aggregator=NQLAggregator.AND,
            ),
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
        )
