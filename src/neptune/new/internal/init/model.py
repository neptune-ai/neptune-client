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
__all__ = ["init_model"]

import os
import threading
from typing import Optional

from neptune.common.exceptions import NeptuneException
from neptune.new.attributes import constants as attr_consts
from neptune.new.envs import CONNECTION_MODE
from neptune.new.exceptions import (
    NeedExistingModelForReadOnlyMode,
    NeptuneMissingRequiredInitParameter,
    NeptuneModelKeyAlreadyExistsError,
    NeptuneObjectCreationConflict,
)
from neptune.new.internal import id_formats
from neptune.new.internal.backends.factory import get_backend
from neptune.new.internal.backends.project_name_lookup import project_name_lookup
from neptune.new.internal.backgroud_job_list import BackgroundJobList
from neptune.new.internal.id_formats import QualifiedName
from neptune.new.internal.init.parameters import (
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.new.internal.operation_processors.factory import get_operation_processor
from neptune.new.internal.utils import verify_type
from neptune.new.internal.utils.deprecation import deprecated_parameter
from neptune.new.internal.utils.ping_background_job import PingBackgroundJob
from neptune.new.metadata_containers import Model
from neptune.new.types.mode import Mode


@deprecated_parameter(deprecated_kwarg_name="model", required_kwarg_name="with_id")
def init_model(
    *,
    with_id: Optional[str] = None,
    name: Optional[str] = None,
    key: Optional[str] = None,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    mode: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
) -> Model:
    verify_type("with_id", with_id, (str, type(None)))
    verify_type("name", name, (str, type(None)))
    verify_type("key", key, (str, type(None)))
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("mode", mode, (str, type(None)))
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))

    # make mode proper Enum instead of string
    mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

    if mode == Mode.OFFLINE:
        raise NeptuneException("Model can't be initialized in OFFLINE mode")

    name = DEFAULT_NAME if with_id is None and name is None else name

    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)

    if mode == Mode.OFFLINE or mode == Mode.DEBUG:
        project = OFFLINE_PROJECT_QUALIFIED_NAME

    project = id_formats.conform_optional(project, QualifiedName)
    project_obj = project_name_lookup(backend=backend, name=project)
    project = f"{project_obj.workspace}/{project_obj.name}"

    if with_id is not None:
        # with_id (resume existing model) has priority over key (creating a new model)
        #  additional creation parameters (e.g. name) are simply ignored in this scenario
        model_id = QualifiedName(project + "/" + with_id)
        api_model = backend.get_metadata_container(container_id=model_id, expected_container_type=Model.container_type)
    elif key is not None:
        if mode == Mode.READ_ONLY:
            raise NeedExistingModelForReadOnlyMode()

        try:
            api_model = backend.create_model(project_id=project_obj.id, key=key)
        except NeptuneObjectCreationConflict as e:
            base_url = backend.get_display_address()
            raise NeptuneModelKeyAlreadyExistsError(
                model_key=key,
                models_tab_url=f"{base_url}/{project_obj.workspace}/{project_obj.name}/models",
            ) from e

    else:
        raise NeptuneMissingRequiredInitParameter(
            parameter_name="key",
            called_function="init_model",
        )

    model_lock = threading.RLock()

    operation_processor = get_operation_processor(
        mode=mode,
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
        id_=api_model.id,
        mode=mode,
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

    _model._startup(debug_mode=mode == Mode.DEBUG)

    return _model
