#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

import logging
import threading
from typing import Optional

from neptune.new.attributes import constants as attr_consts
from neptune.new.exceptions import (
    NeedExistingModelForReadOnlyMode,
)
from neptune.new.internal.backends.factory import get_backend
from neptune.new.internal.backends.project_name_lookup import project_name_lookup
from neptune.new.internal.backgroud_job_list import BackgroundJobList
from neptune.new.internal.operation_processors.factory import get_operation_processor
from neptune.new.internal.utils import verify_type
from neptune.new.internal.utils.ping_background_job import PingBackgroundJob
from neptune.new.model import Model
from neptune.new.types.mode import Mode
from neptune.new.version import version as parsed_version

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)


def init_model(
    *,
    model: Optional[str] = None,
    name: Optional[str] = None,
    key: Optional[str] = None,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    mode: str = Mode.ASYNC.value,
    flush_period: float = 5,
    proxies: Optional[dict] = None,
) -> Model:
    verify_type("model", model, (str, type(None)))
    verify_type("name", name, (str, type(None)))
    verify_type("key", key, (str, type(None)))
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("mode", mode, str)
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))
    mode = Mode(mode)

    name = "Untitled" if model is None and name is None else name

    backend = get_backend(mode, api_token=api_token, proxies=proxies)

    if mode == Mode.OFFLINE or mode == Mode.DEBUG:
        project = "offline/project-placeholder"

    project_obj = project_name_lookup(backend, project)
    project = f"{project_obj.workspace}/{project_obj.name}"

    if model:
        api_model = backend.get_model(project + "/" + model)
    else:
        if mode == Mode.READ_ONLY:
            raise NeedExistingModelForReadOnlyMode()

        api_model = backend.create_model(project_id=project_obj.id, key=key)

    model_lock = threading.RLock()

    operation_processor = get_operation_processor(
        mode,
        container_id=api_model.id,
        container_type=Model.container_type,
        backend=backend,
        lock=model_lock,
        flush_period=flush_period,
    )

    background_jobs = []
    if mode != Mode.READ_ONLY:
        background_jobs.append(PingBackgroundJob())

    _model = Model(
        _id=api_model.id,
        backend=backend,
        op_processor=operation_processor,
        background_job=BackgroundJobList(background_jobs),
        lock=model_lock,
        workspace=api_model.workspace,
        project_name=api_model.project_name,
        sys_id=api_model.sys_id,
        project_id=project_obj.id,
    )
    if mode != Mode.OFFLINE:
        _model.sync(wait=False)

    if mode != Mode.READ_ONLY:
        if name is not None:
            _model[attr_consts.SYSTEM_NAME_ATTRIBUTE_PATH] = name

    # pylint: disable=protected-access
    _model._startup(debug_mode=mode == Mode.DEBUG)

    return _model
