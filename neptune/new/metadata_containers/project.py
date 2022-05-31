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
from typing import Any, Dict, Iterable, Optional, Union

from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLEmptyQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
)
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import SysId, UniqueId
from neptune.new.internal.operation_processors.operation_processor import (
    OperationProcessor,
)
from neptune.new.internal.utils import as_list
from neptune.new.metadata_containers import MetadataContainer
from neptune.new.metadata_containers.metadata_containers_table import Table
from neptune.new.types.mode import Mode


class Project(MetadataContainer):
    """A class for managing a Neptune project and retrieving information from it.

    You may also want to check `Project docs page`_.

    .. _Project docs page:
       https://docs.neptune.ai/api-reference/project
    """

    container_type = ContainerType.PROJECT

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
    ):
        super().__init__(
            id_=id_,
            mode=mode,
            backend=backend,
            op_processor=op_processor,
            background_job=background_job,
            lock=lock,
            project_id=id_,
            project_name=project_name,
            workspace=workspace,
            sys_id=sys_id,
        )

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api-reference/project#.stop"

    @property
    def _label(self) -> str:
        return f"{self._workspace}/{self._project_name}"

    @property
    def _url(self) -> str:
        return self._backend.get_project_url(
            project_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
        )

    @property
    def _metadata_url(self) -> str:
        return self._url.rstrip("/") + "/metadata"

    # pylint:disable=redefined-builtin
    def fetch_runs_table(
        self,
        id: Optional[Union[str, Iterable[str]]] = None,
        state: Optional[Union[str, Iterable[str]]] = None,
        owner: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[Union[str, Iterable[str]]] = None,
    ) -> Table:
        """Retrieve runs matching the specified criteria.

        All parameters are optional, each of them specifies a single criterion.
        Only runs matching all of the criteria will be returned.

        Args:
            id (str or list of str, optional): A run's id or list of ids.
                E.g. `'SAN-1'` or `['SAN-1', 'SAN-2']`.
                Matching any element of the list is sufficient to pass the criterion.
                Defaults to `None`.
            state (str or list of str, optional): A run's state like or list of states.
                E.g. `'running'` or `['idle', 'running']`.
                Possible values: 'idle', 'running'.
                Defaults to `None`.
                Matching any element of the list is sufficient to pass the criterion.
            owner (str or list of str, optional): Username of the run's owner  or a list of owners.
                E.g. 'josh' or ['frederic', 'josh'].
                The user who created the tracked run is an owner.
                Defaults to `None`.
                Matching any element of the list is sufficient to pass the criterion.
            tag (str or list of str, optional): An experiment tag or list of tags.
                E.g. `'lightGBM'` or ['pytorch', 'cycleLR'].
                Defaults to `None`.
                Only experiments that have all specified tags will match this criterion.

        Returns:
            ``Table``: object containing experiments matching the specified criteria.

            Use `.to_pandas()` to convert it to Pandas `DataFrame`.

        Examples:
            >>> import neptune.new as neptune

            >>> # Fetch project 'jackie/sandbox'
            ... project = neptune.get_project(name='jackie/sandbox')

            >>> # Fetch all Runs metadata as Pandas DataFrame
            ... runs_table_df = project.fetch_runs_table().to_pandas()

            >>> # Sort runs by creation time
            ... runs_table_df = runs_table_df.sort_values(by='sys/creation_time', ascending=False)

            >>> # Extract the last runs id
            ... last_run_id = runs_table_df['sys/id'].values[0]

            You can also filter the runs table by state, owner or tag or a combination:

            >>> # Fetch only inactive runs
            ... runs_table_df = project.fetch_runs_table(state='idle').to_pandas()

            >>> # Fetch only runs created by CI service
            ... runs_table_df = project.fetch_runs_table(owner='my_company_ci_service').to_pandas()

            >>> # Fetch only runs that have both 'Exploration' and 'Optuna' tag
            ... runs_table_df = project.fetch_runs_table(tag=['Exploration', 'Optuna']).to_pandas()

            >>> # You can combine conditions. Runs satisfying all conditions will be fetched
            ... runs_table_df = project.fetch_runs_table(state='idle', tag='Exploration').to_pandas()

        You may also want to check `fetch_runs_table docs page`_.

        .. _fetch_runs_table docs page:
            https://docs.neptune.ai/api-reference/project#fetch_runs_table
        """
        ids = as_list("id", id)
        states = as_list("state", state)
        owners = as_list("owner", owner)
        tags = as_list("tag", tag)

        query_items = []

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

        return MetadataContainer._fetch_entries(self, child_type=ContainerType.RUN, query=query)

    def fetch_models_table(self) -> Table:
        """Retrieve models

        Returns:
            ``Table``: object containing models matching the specified criteria.

            Use `.to_pandas()` to convert it to Pandas `DataFrame`.

        Examples:
            >>> import neptune.new as neptune

            >>> # Fetch project 'jackie/sandbox'
            ... project = neptune.get_project(name='jackie/sandbox')

            >>> # Fetch all Models metadata as Pandas DataFrame
            ... models_table_df = project.fetch_models_table().to_pandas()

            >>> # Sort models by creation time
            ... models_table_df = models_table_df.sort_values(by='sys/creation_time', ascending=False)

            >>> # Extract the last model id
            ... last_model_id = models_table_df['sys/id'].values[0]

        You may also want to check `fetch_models_table docs page`_.

        .. _fetch_runs_table docs page:
            https://docs.neptune.ai/api-reference/project#fetch_models_table
        """
        return MetadataContainer._fetch_entries(
            self, child_type=ContainerType.MODEL, query=NQLEmptyQuery()
        )

    def assign(self, value, wait: bool = False) -> None:
        """Assign values to multiple fields from a dictionary.
        You can use this method to log multiple pieces of information with one command.
        Args:
            value (dict): A dictionary with values to assign, where keys become the paths of the fields.
                The dictionary can be nested - in such case the path will be a combination of all keys.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `False`.
        Examples:
            >>> import neptune.new as neptune
            >>> project = neptune.init_project(name="MY_WORKSPACE/MY_PROJECT")
            >>> # Assign multiple fields from a dictionary
            ... general_info = {"brief": URL_TO_PROJECT_BRIEF, "deadline": "2049-06-30"}
            >>> project["general"] = general_info
            >>> # You can always log explicitly parameters one by one
            ... project["general/brief"] = URL_TO_PROJECT_BRIEF
            >>> project["general/deadline"] = "2049-06-30"
            >>> # Dictionaries can be nested
            ... general_info = {"brief": {"url": URL_TO_PROJECT_BRIEF}}
            >>> project["general"] = general_info
            >>> # This will log the url under path "general/brief/url"
        You may also want to check `assign docs page`_.
        .. _assign docs page:
            https://docs.neptune.ai/api-reference/project#.assign
        """
        return MetadataContainer.assign(self, value=value, wait=wait)

    def fetch(self) -> dict:
        """Fetch values of all non-File Atom fields as a dictionary.
        The result will preserve the hierarchical structure of the projects's metadata
        but will contain only non-File Atom fields.
        Returns:
            `dict` containing all non-File Atom fields values.
        Examples:
            >>> import neptune.new as neptune
            >>> project = neptune.init_project(name="MY_WORKSPACE/MY_PROJECT")
            >>> # Fetch all the project metrics
            >>> project_metrics = project["metrics"].fetch()
        You may also want to check `fetch docs page`_.
        .. _fetch docs page:
            https://docs.neptune.ai/api-reference/project#.fetch
        """
        return MetadataContainer.fetch(self)

    def stop(self, seconds: Optional[Union[float, int]] = None) -> None:
        """Stops the connection to the project and kills the synchronization thread.
        `.stop()` will be automatically called when a script that initialized the connection finishes
        or on the destruction of Neptune context.
        When using Neptune with Jupyter notebooks it's a good practice to stop the connection manually as it
        will be stopped automatically only when the Jupyter kernel stops.
        Args:
            seconds (int or float, optional): Seconds to wait for all tracking calls to finish
                before stopping the tracked run.
                If `None` will wait for all tracking calls to finish. Defaults to `True`.
        Examples:
            If you are initializing the connection from a script you don't need to call `.stop()`:
            >>> import neptune.new as neptune
            >>> project = neptune.init_project(name="MY_WORKSPACE/MY_PROJECT")
            >>> # Your code
            ... pass
            ... # If you are executing Python script .stop()
            ... # is automatically called at the end for every Neptune object
            If you are initializing multiple connection from one script it is a good practice
            to .stop() the unneeded connections. You can also use Context Managers - Neptune
            will automatically call .stop() on the destruction of Project context:
            >>> import neptune.new as neptune
            >>> # If you are initializing multiple connections from the same script
            ... # stop the connection manually once not needed
            ... for project_name in projects:
            ...   project = neptune.init_project(name=project_name)
            ...   # Your code
            ...   pass
            ...   project.stop()
            >>> # You can also use with statement and context manager
            ... for project_name in projects:
            ...   with neptune.init_project(name=project_name) as project:
            ...     # Your code
            ...     pass
            ...     # .stop() is automatically called
            ...     # when code execution exits the with statement
        .. warning::
            If you are using Jupyter notebooks for connecting to a project you need to manually invoke `.stop()`
            once the connection is not needed.
        You may also want to check `stop docs page`_.
        .. _stop docs page:
            https://docs.neptune.ai/api-reference/project#.stop
        """
        return MetadataContainer.stop(self, seconds=seconds)

    def get_structure(self) -> Dict[str, Any]:
        """Returns a project's metadata structure in form of a dictionary.
        This method can be used to traverse the project's metadata structure programmatically
        when using Neptune in automated workflows.
        .. danger::
            The returned object is a shallow copy of an internal structure.
            Any modifications to it may result in tracking malfunction.
        Returns:
            ``dict``: with the project's metadata structure.
        """
        return MetadataContainer.get_structure(self)

    def print_structure(self) -> None:
        """Pretty prints the structure of the project's metadata.
        Paths are ordered lexicographically and the whole structure is neatly colored.
        """
        return MetadataContainer.print_structure(self)

    def pop(self, path: str, wait: bool = False) -> None:
        """Removes the field or whole namespace stored under the path completely and all data associated with them.
        Args:
            path (str): Path of the field or namespace to be removed.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `False`.
        Examples:
            >>> import neptune.new as neptune
            >>> project = neptune.init_project(name="MY_WORKSPACE/MY_PROJECT")
            >>> # Delete a field along with it's data
            ... project.pop("datasets/v0.4")
            >>> # .pop() can be invoked directly on fields and namespaces
            >>> project['parameters/learning_rate'] = 0.3
            >>> # Following line
            ... project.pop("datasets/v0.4")
            >>> # is equiavlent to this line
            ... project["datasets/v0.4"].pop()
            >>> # or this line
            ... project["datasets"].pop("v0.4")
            >>> # You can also delete in batch whole namespace
            ... project["datasets"].pop()
        You may also want to check `pop docs page`_.
        .. _pop docs page:
           https://docs.neptune.ai/api-reference/project#.pop
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
            https://docs.neptune.ai/api-reference/project#.wait
        """
        return MetadataContainer.wait(self, disk_only=disk_only)

    def sync(self, wait: bool = True) -> None:
        """Synchronizes local representation of the project with Neptune servers.
        Args:
            wait (bool, optional, default is True): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `True`.
        You may also want to check `sync docs page`_.
        .. _sync docs page:
            https://docs.neptune.ai/api-reference/project#.sync
        """
        return MetadataContainer.sync(self, wait=wait)
