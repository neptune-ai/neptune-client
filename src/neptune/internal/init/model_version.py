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
__all__ = ["init_model_version"]

import os
import threading
from typing import Optional

from neptune.attributes import constants as attr_consts
from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.exceptions import (
    NeedExistingModelVersionForReadOnlyMode,
    NeptuneMissingRequiredInitParameter,
)
from neptune.internal import id_formats
from neptune.internal.backends.factory import get_backend
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.utils import verify_type
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.metadata_containers import (
    Model,
    ModelVersion,
)
from neptune.types.mode import Mode


def init_model_version(
    with_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    mode: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
) -> ModelVersion:
    verify_type("model", model, (str, type(None)))
    verify_type("with_id", with_id, (str, type(None)))
    verify_type("name", name, (str, type(None)))
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("mode", mode, (str, type(None)))
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))

    # make mode proper Enum instead of string
    mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)
    name = DEFAULT_NAME if model is None and name is None else name

    if mode == Mode.OFFLINE or mode == Mode.DEBUG:
        project = OFFLINE_PROJECT_QUALIFIED_NAME
    project = id_formats.conform_optional(project, QualifiedName)

    if mode == Mode.OFFLINE:
        raise NeptuneException("ModelVersion can't be initialized in OFFLINE mode")

    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)

    project_obj = project_name_lookup(backend=backend, name=project)

    api_object = get_or_create_api_object(
        project_obj=project_obj, backend=backend, with_id=with_id, model=model, mode=mode
    )

    lock = threading.RLock()

    operation_processor = get_operation_processor(
        mode=mode,
        container_id=api_object.id,
        container_type=ModelVersion.container_type,
        backend=backend,
        lock=lock,
        flush_period=flush_period,
    )

    _object = ModelVersion(
        id_=api_object.id,
        mode=mode,
        backend=backend,
        op_processor=operation_processor,
        background_job=background_jobs(mode=mode),
        lock=lock,
        workspace=api_object.workspace,
        project_name=api_object.project_name,
        sys_id=api_object.sys_id,
        project_id=project_obj.id,
    )

    if mode != Mode.OFFLINE:
        _object.sync(wait=False)

    additional_attributes(_object=_object, mode=mode, name=name)

    _object._startup(debug_mode=mode == Mode.DEBUG)

    return _object


def get_or_create_api_object(project_obj, backend, with_id, model, mode):
    project = f"{project_obj.workspace}/{project_obj.name}"
    if with_id is not None:
        # with_id (resume existing model_version) has priority over model (creating a new model_version)
        version_id = QualifiedName(project + "/" + with_id)
        return backend.get_metadata_container(
            container_id=version_id, expected_container_type=ModelVersion.container_type
        )
    elif model is not None:
        if mode == Mode.READ_ONLY:
            raise NeedExistingModelVersionForReadOnlyMode()

        model_id = QualifiedName(project + "/" + model)
        api_model = backend.get_metadata_container(container_id=model_id, expected_container_type=Model.container_type)
        return backend.create_model_version(project_id=project_obj.id, model_id=api_model.id)
    else:
        raise NeptuneMissingRequiredInitParameter(
            parameter_name="model",
            called_function="init_model_version",
        )


def background_jobs(mode):
    if mode != Mode.READ_ONLY:
        return BackgroundJobList([PingBackgroundJob()])
    return BackgroundJobList([])


def additional_attributes(_object, mode, name):
    if mode != Mode.READ_ONLY:
        if name is not None:
            _object[attr_consts.SYSTEM_NAME_ATTRIBUTE_PATH] = name
