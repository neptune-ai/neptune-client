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
from typing import Optional

from neptune.new.exceptions import NeptuneException
from neptune.new.internal import id_formats
from neptune.new.internal.backends.factory import get_backend
from neptune.new.internal.backends.project_name_lookup import project_name_lookup
from neptune.new.internal.backgroud_job_list import BackgroundJobList
from neptune.new.internal.id_formats import QualifiedName
from neptune.new.internal.init.parameters import DEFAULT_FLUSH_PERIOD
from neptune.new.internal.operation_processors.factory import get_operation_processor
from neptune.new.internal.utils import verify_type
from neptune.new.metadata_containers import Project
from neptune.new.types.mode import Mode


def init_project(
    *,
    name: Optional[str] = None,
    api_token: Optional[str] = None,
    mode: str = Mode.ASYNC.value,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
) -> Project:
    verify_type("name", name, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("mode", mode, str)
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))
    # make mode proper Enum instead of string
    mode = Mode(mode)

    if mode == Mode.OFFLINE:
        raise NeptuneException("Project can't be initialized in OFFLINE mode")

    name = id_formats.conform_optional(name, QualifiedName)
    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)
    project_obj = project_name_lookup(backend=backend, name=name)

    project_lock = threading.RLock()

    operation_processor = get_operation_processor(
        mode=mode,
        container_id=project_obj.id,
        container_type=Project.container_type,
        backend=backend,
        lock=project_lock,
        flush_period=flush_period,
    )

    background_jobs = []

    project = Project(
        id_=project_obj.id,
        backend=backend,
        op_processor=operation_processor,
        background_job=BackgroundJobList(background_jobs),
        lock=project_lock,
        workspace=project_obj.workspace,
        project_name=project_obj.name,
        sys_id=project_obj.sys_id,
    )

    if mode != Mode.OFFLINE:
        project.sync(wait=False)

    # pylint: disable=protected-access
    project._startup(debug_mode=mode == Mode.DEBUG)
    return project


def get_project(
    name: Optional[str] = None,
    api_token: Optional[str] = None,
    proxies: Optional[dict] = None,
) -> Project:
    """Get a project with given `name`.

    Args:
        name(str, optional): Name of a project in a form of namespace/project_name. Defaults to `None`.
            If None, the value of `NEPTUNE_PROJECT` environment variable will be taken.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If None, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.

    Returns:
        ``Project``: object that can be used to interact with the project as a whole like fetching data from Runs table.

    Examples:

        >>> import neptune.new as neptune

        >>> # Fetch project 'jack/sandbox'
        ... project = neptune.get_project(name='jack/sandbox')

        >>> # Fetch all Runs metadata as Pandas DataFrame
        ... runs_table_df = project.fetch_runs_table().to_pandas()

    You may also want to check `get_project docs page`_.

    .. _get_project docs page:
       https://docs.neptune.ai/api-reference/neptune#.get_project
    """
    return init_project(
        name=name, api_token=api_token, mode=Mode.READ_ONLY.value, proxies=proxies
    )
