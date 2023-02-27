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
__all__ = ["ModelVersion"]

import os
from typing import Optional

from neptune.attributes.constants import (
    SYSTEM_NAME_ATTRIBUTE_PATH,
    SYSTEM_STAGE_ATTRIBUTE_PATH,
)
from neptune.common.exceptions import NeptuneException
from neptune.envs import CONNECTION_MODE
from neptune.exceptions import (
    InactiveModelVersionException,
    NeedExistingModelVersionForReadOnlyMode,
    NeptuneMissingRequiredInitParameter,
    NeptuneOfflineModeChangeStageException,
)
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.metadata_containers import MetadataContainer
from neptune.types.mode import Mode
from neptune.types.model_version_stage import ModelVersionStage


class ModelVersion(MetadataContainer):
    """A class for managing a Neptune model version and retrieving information from it.

    You may also want to check `ModelVersion docs page`_.

    .. _ModelVersion docs page:
       https://docs.neptune.ai/api/model_version
    """

    container_type = ContainerType.MODEL_VERSION

    def __init__(
        self,
        with_id: Optional[str] = None,
        *,
        name: Optional[str] = None,
        model: Optional[str] = None,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Optional[str] = None,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
    ):
        verify_type("with_id", with_id, (str, type(None)))
        verify_type("name", name, (str, type(None)))
        verify_type("model", model, (str, type(None)))
        verify_type("project", project, (str, type(None)))
        verify_type("mode", mode, (str, type(None)))

        self._model: Optional[str] = model
        self._with_id: Optional[str] = with_id
        self._name: Optional[str] = DEFAULT_NAME if model is None and name is None else name

        # make mode proper Enum instead of string
        mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

        if mode == Mode.OFFLINE:
            raise NeptuneException("ModelVersion can't be initialized in OFFLINE mode")

        if mode == Mode.DEBUG:
            project = OFFLINE_PROJECT_QUALIFIED_NAME

        super().__init__(project=project, api_token=api_token, mode=mode, flush_period=flush_period, proxies=proxies)

    def _get_or_create_api_object(self) -> ApiExperiment:
        project_workspace = self._project_api_object.workspace
        project_name = self._project_api_object.name
        project_qualified_name = f"{project_workspace}/{project_name}"

        if self._with_id is not None:
            # with_id (resume existing model_version) has priority over model (creating a new model_version)
            return self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._with_id),
                expected_container_type=self.container_type,
            )
        elif self._model is not None:
            if self._mode == Mode.READ_ONLY:
                raise NeedExistingModelVersionForReadOnlyMode()

            api_model = self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._model),
                expected_container_type=ContainerType.MODEL,
            )
            return self._backend.create_model_version(project_id=self._project_api_object.id, model_id=api_model.id)
        else:
            raise NeptuneMissingRequiredInitParameter(
                parameter_name="model",
                called_function="init_model_version",
            )

    def _prepare_background_jobs(self) -> BackgroundJobList:
        return BackgroundJobList([PingBackgroundJob()])

    def _write_initial_attributes(self):
        if self._name is not None:
            self[SYSTEM_NAME_ATTRIBUTE_PATH] = self._name

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveModelVersionException(label=self._sys_id)

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_model_version_url(
            model_version_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
            model_id=self["sys/model_id"].fetch(),
        )

    def change_stage(self, stage: str):
        mapped_stage = ModelVersionStage(stage)

        if isinstance(self._op_processor, OfflineOperationProcessor):
            raise NeptuneOfflineModeChangeStageException()

        self.wait()

        with self.lock():
            attr = self.get_attribute(SYSTEM_STAGE_ATTRIBUTE_PATH)
            # We are sure that such attribute exists, because
            # SYSTEM_STAGE_ATTRIBUTE_PATH is set by default on ModelVersion creation
            assert attr is not None, f"No {SYSTEM_STAGE_ATTRIBUTE_PATH} found in model version"
            attr.process_assignment(
                value=mapped_stage.value,
                wait=True,
            )
