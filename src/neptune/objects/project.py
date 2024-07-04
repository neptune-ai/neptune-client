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

from typing import (
    Iterable,
    Optional,
    Union,
)

from typing_extensions import Literal

from neptune.exceptions import NeptuneUnsupportedFunctionalityException
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.container_type import ContainerType
from neptune.internal.utils import (
    as_list,
    verify_collection_type,
    verify_type,
    verify_value,
)
from neptune.objects.mode import Mode
from neptune.objects.utils import (
    build_raw_query,
    prepare_nql_query,
)
from neptune.objects.with_backend import WithBackend
from neptune.table import Table
from neptune.typing import (
    ProgressBarCallback,
    ProgressBarType,
)


class Project(WithBackend):
    """Starts a connection to an existing Neptune project.

    You can use the Project object to retrieve information about runs, models, and model versions
    within the project.

    You can also log (and fetch) metadata common to the whole project, such as information about datasets,
    links to documents, or key project metrics.

    You can also use the Project object as a context manager (see examples).

    Args:
        project: Name of a project in the form `workspace-name/project-name`.
            If left empty, the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token.
            If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        mode: Connection mode in which the tracking will work.
            If left empty, the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, "async" is used by default.
            Possible values are `async`, `sync`, `read-only`, and `debug`.
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.

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
        mode: Optional[Literal["async", "sync", "read-only", "debug"]] = None,
        proxies: Optional[dict] = None,
    ):
        mode = mode or Mode.READ_ONLY.value

        if mode != Mode.READ_ONLY.value:
            raise NeptuneUnsupportedFunctionalityException

        mode = Mode(mode)

        super().__init__(
            project=project,
            api_token=api_token,
            mode=mode,
            proxies=proxies,
        )

    def _fetch_entries(
        self,
        child_type: ContainerType,
        query: NQLQuery,
        columns: Optional[Iterable[str]],
        limit: Optional[int],
        sort_by: str,
        ascending: bool,
        progress_bar: Optional[ProgressBarType],
    ) -> Table:
        if columns is not None:
            # always return entries with 'sys/id' and the column chosen for sorting when filter applied
            columns = set(columns)
            columns.add("sys/id")
            columns.add(sort_by)

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[child_type],
            query=query,
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
        )

        return Table(
            backend=self._backend,
            container_type=child_type,
            entries=leaderboard_entries,
        )

    def fetch_runs_table(
        self,
        *,
        query: Optional[str] = None,
        id: Optional[Union[str, Iterable[str]]] = None,
        state: Optional[Union[Literal["inactive", "active"], Iterable[Literal["inactive", "active"]]]] = None,
        owner: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[Union[str, Iterable[str]]] = None,
        columns: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> Table:
        """Retrieve runs matching the specified criteria.

        All parameters are optional. Each of them specifies a single criterion.
        Only runs matching all of the criteria will be returned.

        Args:
            query: NQL query string. Syntax: https://docs.neptune.ai/usage/nql/
                Example: `"(accuracy: float > 0.88) AND (loss: float < 0.2)"`.
                Exclusive with the `id`, `state`, `owner`, and `tag` parameters.
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
            columns: Names of columns to include in the table, as a list of field names.
                The Neptune ID ("sys/id") is included automatically.
                If `None` (default), all the columns of the runs table are included, up to a maximum of 10 000 columns.
            trashed: Whether to retrieve trashed runs.
                If `True`, only trashed runs are retrieved.
                If `False` (default), only not-trashed runs are retrieved.
                If `None`, both trashed and not-trashed runs are retrieved.
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.

        Returns:
            `Table` object containing `Run` objects matching the specified criteria.

            Use `to_pandas()` to convert the table to a pandas DataFrame.

        Examples:
            >>> import neptune
            ... # Fetch project "jackie/sandbox"
            ... project = neptune.init_project(mode="read-only", project="jackie/sandbox")

            >>> # Fetch the metadata of all runs as a pandas DataFrame
            ... runs_table_df = project.fetch_runs_table().to_pandas()
            ... # Extract the ID of the last run
            ... last_run_id = runs_table_df["sys/id"].values[0]

            >>> # Fetch the 100 oldest runs
            ... runs_table_df = project.fetch_runs_table(
            ...     sort_by="sys/creation_time", ascending=True, limit=100
            ... ).to_pandas()

            >>> # Fetch the 100 largest runs (space they take up in Neptune)
            ... runs_table_df = project.fetch_runs_table(sort_by="sys/size", limit=100).to_pandas()

            >>> # Include only the fields "train/loss" and "params/lr" as columns:
            ... runs_table_df = project.fetch_runs_table(columns=["params/lr", "train/loss"]).to_pandas()

            >>> # Pass a custom progress bar callback
            ... runs_table_df = project.fetch_runs_table(progress_bar=MyProgressBar).to_pandas()
            ... # The class MyProgressBar(ProgressBarCallback) must be defined

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

        if any((id, state, owner, tag)) and query is not None:
            raise ValueError(
                "You can't use the 'query' parameter together with the 'id', 'state', 'owner', or 'tag' parameters."
            )

        ids = as_list("id", id)
        states = as_list("state", state)
        owners = as_list("owner", owner)
        tags = as_list("tag", tag)

        verify_type("query", query, (str, type(None)))
        verify_type("trashed", trashed, (bool, type(None)))
        verify_type("limit", limit, (int, type(None)))
        verify_type("sort_by", sort_by, str)
        verify_type("ascending", ascending, bool)
        verify_type("progress_bar", progress_bar, (type(None), bool, type(ProgressBarCallback)))
        verify_collection_type("state", states, str)

        if isinstance(limit, int) and limit <= 0:
            raise ValueError(f"Parameter 'limit' must be a positive integer or None. Got {limit}.")

        for state in states:
            verify_value("state", state.lower(), ("inactive", "active"))

        if query is not None:
            nql_query = build_raw_query(query, trashed=trashed)
        else:
            nql_query = prepare_nql_query(ids, states, owners, tags, trashed)

        return self._fetch_entries(
            child_type=ContainerType.RUN,
            query=nql_query,
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
        )

    def fetch_models_table(
        self,
        *,
        query: Optional[str] = None,
        columns: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> Table:
        """Retrieve models stored in the project.

        Args:
            query: NQL query string. Syntax: https://docs.neptune.ai/usage/nql/
                Example: `"(model_size: float > 100) AND (backbone: string = VGG)"`.
            trashed: Whether to retrieve trashed models.
                If `True`, only trashed models are retrieved.
                If `False`, only not-trashed models are retrieved.
                If `None`, both trashed and not-trashed models are retrieved.
            columns: Names of columns to include in the table, as a list of field names.
                The Neptune ID ("sys/id") is included automatically.
                If `None`, all the columns of the models table are included, up to a maximum of 10 000 columns.
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.

        Returns:
            `Table` object containing `Model` objects.

            Use `to_pandas()` to convert the table to a pandas DataFrame.

        Examples:
            >>> import neptune
            ... # Fetch project "jackie/sandbox"
            ... project = neptune.init_project(mode="read-only", project="jackie/sandbox")

            >>> # Fetch the metadata of all models as a pandas DataFrame
            ... models_table_df = project.fetch_models_table().to_pandas()

            >>> # Include only the fields "params/lr" and "info/size" as columns:
            ... models_table_df = project.fetch_models_table(columns=["params/lr", "info/size"]).to_pandas()

            >>> # Fetch 10 oldest model objects
            ... models_table_df = project.fetch_models_table(
            ...     sort_by="sys/creation_time", ascending=True, limit=10
            ...  ).to_pandas()
            ... # Extract the ID of the first listed (oldest) model object
            ... last_model_id = models_table_df["sys/id"].values[0]

            >>> # Fetch models with VGG backbone
            ... models_table_df = project.fetch_models_table(
                    query="(backbone: string = VGG)"
                ).to_pandas()

        See also the API reference in the docs:
            https://docs.neptune.ai/api/project#fetch_models_table
        """
        verify_type("query", query, (str, type(None)))
        verify_type("limit", limit, (int, type(None)))
        verify_type("sort_by", sort_by, str)
        verify_type("ascending", ascending, bool)
        verify_type("progress_bar", progress_bar, (type(None), bool, type(ProgressBarCallback)))

        if isinstance(limit, int) and limit <= 0:
            raise ValueError(f"Parameter 'limit' must be a positive integer or None. Got {limit}.")

        query = query if query is not None else ""
        nql = build_raw_query(query=query, trashed=trashed)
        return self._fetch_entries(
            child_type=ContainerType.MODEL,
            query=nql,
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
        )
