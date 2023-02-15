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

from neptune.attributes.constants import SYSTEM_STAGE_ATTRIBUTE_PATH
from neptune.exceptions import (
    InactiveModelVersionException,
    NeptuneOfflineModeChangeStageException,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.internal.state import ContainerState
from neptune.metadata_containers import MetadataContainer
from neptune.types.model_version_stage import ModelVersionStage


class ModelVersion(MetadataContainer):
    """A class for managing a Neptune model version and retrieving information from it.

    You may also want to check `ModelVersion docs page`_.

    .. _ModelVersion docs page:
       https://docs.neptune.ai/api/model_version
    """

    container_type = ContainerType.MODEL_VERSION

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
