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

from neptune.exceptions import InactiveProjectException
from neptune.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQueryAggregate,
    NQLQueryAttribute,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.state import ContainerState
from neptune.internal.utils import as_list
from neptune.metadata_containers import MetadataContainer
from neptune.metadata_containers.metadata_containers_table import Table


class Project(MetadataContainer):
    """A class for managing a Neptune project and retrieving information from it.

    You may also want to check `Project docs page`_.

    .. _Project docs page:
       https://docs.neptune.ai/api/project
    """

    container_type = ContainerType.PROJECT

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveProjectException(label=f"{self._workspace}/{self._project_name}")

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api/project#stop"

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_project_url(
            project_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
        )

    @property
    def _metadata_url(self) -> str:
        return self.get_url().rstrip("/") + "/metadata"

    @staticmethod
    def _prepare_nql_query(ids, states, owners, tags):
        query_items = [
            NQLQueryAttribute(
                name="sys/trashed",
                type=NQLAttributeType.BOOLEAN,
                operator=NQLAttributeOperator.EQUALS,
                value=False,
            )
        ]

        if ids:
            query_items.append(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/id",
                            type=NQLAttributeType.STRING,
                            operator=NQLAttributeOperator.EQUALS,
                            value=api_id,
                        )
                        for api_id in ids
                    ],
                    aggregator=NQLAggregator.OR,
                )
            )

        if states:
            query_items.append(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/state",
                            type=NQLAttributeType.EXPERIMENT_STATE,
                            operator=NQLAttributeOperator.EQUALS,
                            value=state,
                        )
                        for state in states
                    ],
                    aggregator=NQLAggregator.OR,
                )
            )

        if owners:
            query_items.append(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/owner",
                            type=NQLAttributeType.STRING,
                            operator=NQLAttributeOperator.EQUALS,
                            value=owner,
                        )
                        for owner in owners
                    ],
                    aggregator=NQLAggregator.OR,
                )
            )

        if tags:
            query_items.append(
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/tags",
                            type=NQLAttributeType.STRING_SET,
                            operator=NQLAttributeOperator.CONTAINS,
                            value=tag,
                        )
                        for tag in tags
                    ],
                    aggregator=NQLAggregator.AND,
                )
            )

        query = NQLQueryAggregate(items=query_items, aggregator=NQLAggregator.AND)
        return query

    def fetch_runs_table(
        self,
        id: Optional[Union[str, Iterable[str]]] = None,
        state: Optional[Union[str, Iterable[str]]] = None,
        owner: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[Union[str, Iterable[str]]] = None,
        columns: Optional[Iterable[str]] = None,
    ) -> Table:
        """Retrieve runs matching the specified criteria.

        All parameters are optional. Each of them specifies a single criterion.
        Only runs matching all of the criteria will be returned.

        Args:
            id: Neptune ID of a run, or list of several IDs.
                Example: `"SAN-1"` or `["SAN-1", "SAN-2"]`.
                Matching any element of the list is sufficient to pass the criterion.
                Defaults to `None`.
            state: Run state, or list of states.
                Example: `"running"` or `["idle", "running"]`.
                Possible values: "idle", "running".
                Defaults to `None`.
                Matching any element of the list is sufficient to pass the criterion.
            owner: Username of the run owner, or a list of owners.
                Example: `"josh"` or `["frederic", "josh"]`.
                The owner is the user who created the run.
                Defaults to `None`.
                Matching any element of the list is sufficient to pass the criterion.
            tag: A tag or list of tags applied to the run.
                Example: `"lightGBM"` or `["pytorch", "cycleLR"]`.
                Defaults to `None`.
                Only runs that have all specified tags will match this criterion.
            columns: Names of columns to include in the table, as a list of namespace or field names.
                The Neptune ID ("sys/id") is included automatically.
                Examples:
                    Fields: `["params/lr", "params/batch", "train/acc"]` - these fields are included as columns.
                    Namespaces: `["params", "train"]` - all the fields inside the namespaces are included as columns.
                If `None` (default), all the columns of the runs table are included.

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
            ... runs_table_df = project.fetch_runs_table(state="idle").to_pandas()

            >>> # Fetch only runs created by CI service
            ... runs_table_df = project.fetch_runs_table(owner="my_company_ci_service").to_pandas()

            >>> # Fetch only runs that have both "Exploration" and "Optuna" tags
            ... runs_table_df = project.fetch_runs_table(tag=["Exploration", "Optuna"]).to_pandas()

            >>> # You can combine conditions. Runs satisfying all conditions will be fetched
            ... runs_table_df = project.fetch_runs_table(state="idle", tag="Exploration").to_pandas()

        You may also want to check the API reference in the docs:
            https://docs.neptune.ai/api/project#fetch_runs_table
        """
        ids = as_list("id", id)
        states = as_list("state", state)
        owners = as_list("owner", owner)
        tags = as_list("tag", tag)

        nql_query = self._prepare_nql_query(ids, states, owners, tags)

        return MetadataContainer._fetch_entries(
            self,
            child_type=ContainerType.RUN,
            query=nql_query,
            columns=columns,
        )

    def fetch_models_table(self, columns: Optional[Iterable[str]] = None) -> Table:
        """Retrieve models stored in the project.

        Args:
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
                value=False,
            ),
            columns=columns,
        )
