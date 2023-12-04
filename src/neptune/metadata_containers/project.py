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
__all__ = ["Project"]

import os
from typing import (
    Iterable,
    Optional,
    Union,
)

from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.exceptions import InactiveProjectException
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.backends.nql import (
    NQLAttributeOperator,
    NQLAttributeType,
    NQLEmptyQuery,
    NQLQueryAttribute,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_FLUSH_PERIOD,
)
from neptune.internal.state import ContainerState
from neptune.internal.utils import (
    as_list,
    verify_type,
)
from neptune.metadata_containers import MetadataContainer
from neptune.metadata_containers.abstract import NeptuneObjectCallback
from neptune.metadata_containers.metadata_containers_table import Table
from neptune.metadata_containers.utils import prepare_nql_query
from neptune.types.mode import Mode


class Project(MetadataContainer):
    """Starts a connection to an existing Neptune project.

    You can use the Project object to retrieve information about runs, models, and model versions
    within the project.

    You can also log (and fetch) metadata common to the whole project, such as information about datasets,
    links to documents, or key project metrics.

    Note: If you want to instead create a project, use the
    [`management.create_project()`](https://docs.neptune.ai/api/management/#create_project) function.

    You can also use the Project object as a context manager (see examples).

    Args:
        project: Name of a project in the form `workspace-name/project-name`.
            If left empty, the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token.
            If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        mode: Connection mode in which the tracking will work.
            If left empty, the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, "async" is used by default.
            Possible values are `async`, `sync`, `offline`, `read-only`, and `debug`.
        flush_period: In the asynchronous (default) connection mode, how often disk flushing is triggered.
            Defaults to 5 (every 5 seconds).
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.
            For more information about proxies, see the Requests documentation.
        async_lag_callback: Custom callback which is called if the lag between a queued operation and its
            synchronization with the server exceeds the duration defined by `async_lag_threshold`. The callback
            should take a Project object as the argument and can contain any custom code, such as calling `stop()`
            on the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK` environment variable to `TRUE`.
        async_lag_threshold: In seconds, duration between the queueing and synchronization of an operation.
            If a lag callback (default callback enabled via environment variable or custom callback passed to the
            `async_lag_callback` argument) is enabled, the callback is called when this duration is exceeded.
        async_no_progress_callback: Custom callback which is called if there has been no synchronization progress
            whatsoever for the duration defined by `async_no_progress_threshold`. The callback
            should take a Project object as the argument and can contain any custom code, such as calling `stop()`
            on the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK` environment variable to `TRUE`.
        async_no_progress_threshold: In seconds, for how long there has been no synchronization progress since the
            object was initialized. If a no-progress callback (default callback enabled via environment variable or
            custom callback passed to the `async_no_progress_callback` argument) is enabled, the callback is called
            when this duration is exceeded.

    Returns:
        Project object that can be used to interact with the project as a whole,
        like logging or fetching project-level metadata.

    Examples:

        >>> import neptune

        >>> # Connect to the project "classification" in the workspace "ml-team":
        ... project = neptune.init_project(project="ml-team/classification")

        >>> # Or initialize with the constructor
        ... project = Project(project="ml-team/classification")

        >>> # Connect to a project in read-only mode:
        ... project = neptune.init_project(
        ...     project="ml-team/classification",
        ...     mode="read-only",
        ... )

        Using the Project object as context manager:

        >>> with Project(project="ml-team/classification") as project:
        ...     project["metadata"] = some_metadata

    For more, see the docs:
        Initializing a project:
            https://docs.neptune.ai/api/neptune#init_project
        Project class reference:
            https://docs.neptune.ai/api/project/
    """

    container_type = ContainerType.PROJECT

    def __init__(
        self,
        project: Optional[str] = None,
        *,
        api_token: Optional[str] = None,
        mode: Optional[str] = None,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
        async_lag_callback: Optional[NeptuneObjectCallback] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[NeptuneObjectCallback] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
    ):
        verify_type("mode", mode, (str, type(None)))

        # make mode proper Enum instead of string
        mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

        if mode == Mode.OFFLINE:
            raise NeptuneException("Project can't be initialized in OFFLINE mode")

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
        return ApiExperiment(
            id=self._project_api_object.id,
            type=ContainerType.PROJECT,
            sys_id=self._project_api_object.sys_id,
            workspace=self._project_api_object.workspace,
            project_name=self._project_api_object.name,
        )

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveProjectException(label=f"{self._workspace}/{self._project_name}")

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_project_url(
            project_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
        )

    def fetch_runs_table(
        self,
        *,
        id: Optional[Union[str, Iterable[str]]] = None,
        state: Optional[Union[str, Iterable[str]]] = None,
        owner: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[Union[str, Iterable[str]]] = None,
        columns: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
    ) -> Table:
        """Retrieve runs matching the specified criteria.

        All parameters are optional. Each of them specifies a single criterion.
        Only runs matching all of the criteria will be returned.

        Args:
            id: Neptune ID of a run, or list of several IDs.
                Example: `"SAN-1"` or `["SAN-1", "SAN-2"]`.
                Matching any element of the list is sufficient to pass the criterion.
            state: Run state, or list of states.
                Example: `"active"`.
                Possible values: `"inactive"`, `"active"`.
                Matching any element of the list is sufficient to pass the criterion.
            owner: Username of the run owner, or a list of owners.
                Example: `"josh"` or `["frederic", "josh"]`.
                The owner is the user who created the run.
                Matching any element of the list is sufficient to pass the criterion.
            tag: A tag or list of tags applied to the run.
                Example: `"lightGBM"` or `["pytorch", "cycleLR"]`.
                Only runs that have all specified tags will match this criterion.
            columns: Names of columns to include in the table, as a list of namespace or field names.
                The Neptune ID ("sys/id") is included automatically.
                Examples:
                    Fields: `["params/lr", "params/batch", "train/acc"]` - these fields are included as columns.
                    Namespaces: `["params", "train"]` - all the fields inside the namespaces are included as columns.
                If `None` (default), all the columns of the runs table are included.
            trashed: Whether to retrieve trashed runs.
                If `True`, only trashed runs are retrieved.
                If `False` (default), only not-trashed runs are retrieved.
                If `None`, both trashed and not-trashed runs are retrieved.

        Returns:
            `Table` object containing `Run` objects matching the specified criteria.

            Use `to_pandas()` to convert the table to a pandas DataFrame.

        Examples:
            >>> import neptune

            >>> # Fetch project "jackie/sandbox"
            ... project = neptune.init_project(mode="read-only", project="jackie/sandbox")

            >>> # Fetch the metadata of all runs as a pandas DataFrame
            ... runs_table_df = project.fetch_runs_table().to_pandas()

            >>> # Fetch the metadata of all runs as a pandas DataFrame, including only the field "train/loss"
            ... # and the fields from the "params" namespace as columns:
            ... runs_table_df = project.fetch_runs_table(columns=["params", "train/loss"]).to_pandas()

            >>> # Sort runs by creation time
            ... runs_table_df = runs_table_df.sort_values(by="sys/creation_time", ascending=False)

            >>> # Extract the id of the last run
            ... last_run_id = runs_table_df["sys/id"].values[0]

            You can also filter the runs table by state, owner, tag, or a combination of these:

            >>> # Fetch only inactive runs
            ... runs_table_df = project.fetch_runs_table(state="inactive").to_pandas()

            >>> # Fetch only runs created by CI service
            ... runs_table_df = project.fetch_runs_table(owner="my_company_ci_service").to_pandas()

            >>> # Fetch only runs that have both "Exploration" and "Optuna" tags
            ... runs_table_df = project.fetch_runs_table(tag=["Exploration", "Optuna"]).to_pandas()

            >>> # You can combine conditions. Runs satisfying all conditions will be fetched
            ... runs_table_df = project.fetch_runs_table(state="inactive", tag="Exploration").to_pandas()

        See also the API reference in the docs:
            https://docs.neptune.ai/api/project#fetch_runs_table
        """
        ids = as_list("id", id)
        states = as_list("state", state)
        owners = as_list("owner", owner)
        tags = as_list("tag", tag)

        verify_type("trashed", trashed, (bool, type(None)))

        nql_query = prepare_nql_query(ids, states, owners, tags, trashed)

        return MetadataContainer._fetch_entries(
            self,
            child_type=ContainerType.RUN,
            query=nql_query,
            columns=columns,
        )

    def fetch_models_table(self, *, columns: Optional[Iterable[str]] = None, trashed: Optional[bool] = False) -> Table:
        """Retrieve models stored in the project.

        Args:
            trashed: Whether to retrieve trashed models.
                If `True`, only trashed models are retrieved.
                If `False` (default), only not-trashed models are retrieved.
                If `None`, both trashed and not-trashed models are retrieved.
            columns: Names of columns to include in the table, as a list of namespace or field names.
                The Neptune ID ("sys/id") is included automatically.
                Examples:
                    Fields: `["datasets/test", "info/size"]` - these fields are included as columns.
                    Namespaces: `["datasets", "info"]` - all the fields inside the namespaces are included as columns.
                If `None` (default), all the columns of the models table are included.

        Returns:
            `Table` object containing `Model` objects.

            Use `to_pandas()` to convert the table to a pandas DataFrame.

        Examples:
            >>> import neptune

            >>> # Fetch project "jackie/sandbox"
            ... project = neptune.init_project(mode="read-only", project="jackie/sandbox")

            >>> # Fetch the metadata of all models as a pandas DataFrame
            ... models_table_df = project.fetch_models_table().to_pandas()

            >>> # Fetch the metadata of all models as a pandas DataFrame,
            ... # including only the "datasets" namespace and "info/size" field as columns:
            ... models_table_df = project.fetch_models_table(columns=["datasets", "info/size"]).to_pandas()

            >>> # Sort model objects by size
            ... models_table_df = models_table_df.sort_values(by="sys/size")

            >>> # Sort models by creation time
            ... models_table_df = models_table_df.sort_values(by="sys/creation_time", ascending=False)

            >>> # Extract the last model id
            ... last_model_id = models_table_df["sys/id"].values[0]

        You may also want to check the API referene in the docs:
            https://docs.neptune.ai/api/project#fetch_models_table
        """
        return MetadataContainer._fetch_entries(
            self,
            child_type=ContainerType.MODEL,
            query=NQLQueryAttribute(
                name="sys/trashed",
                type=NQLAttributeType.BOOLEAN,
                operator=NQLAttributeOperator.EQUALS,
                value=trashed,
            )
            if trashed is not None
            else NQLEmptyQuery,
            columns=columns,
        )
