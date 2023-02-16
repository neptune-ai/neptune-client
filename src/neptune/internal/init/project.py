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
__all__ = ["init_project"]

import os
import threading
from typing import Optional

from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.internal import id_formats
from neptune.internal.backends.factory import get_backend
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import DEFAULT_FLUSH_PERIOD
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.utils import verify_type
from neptune.metadata_containers import Project
from neptune.types.mode import Mode


def init_project(
    project: Optional[str] = None,
    *,
    api_token: Optional[str] = None,
    mode: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
) -> Project:
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("mode", mode, (str, type(None)))
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))

    # make mode proper Enum instead of string
    mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

    if mode == Mode.OFFLINE:
        raise NeptuneException("Project can't be initialized in OFFLINE mode")

    project = id_formats.conform_optional(project, QualifiedName)
    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)
    project_obj = project_name_lookup(backend=backend, name=project)

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

    npt_project = Project(
        id_=project_obj.id,
        mode=mode,
        backend=backend,
        op_processor=operation_processor,
        background_job=BackgroundJobList(background_jobs),
        lock=project_lock,
        workspace=project_obj.workspace,
        project_name=project_obj.name,
        sys_id=project_obj.sys_id,
    )

    if mode != Mode.OFFLINE:
        npt_project.sync(wait=False)

    npt_project._startup(debug_mode=mode == Mode.DEBUG)
    return npt_project
