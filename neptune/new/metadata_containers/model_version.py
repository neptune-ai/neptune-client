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
import threading

from neptune.new.attribute_container import AttributeContainer
from neptune.new.attributes.constants import SYSTEM_STAGE_ATTRIBUTE_PATH
from neptune.new.exceptions import NeptuneOfflineModeChangeStageException
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation_processors.operation_processor import (
    OperationProcessor,
)
from neptune.new.types.mode import Mode
from neptune.new.types.model_version_stage import ModelVersionStage


class ModelVersion(MetadataContainer):
    """A class for managing a Neptune model version and retrieving information from it.

    You may also want to check `ModelVersion docs page`_.

    .. _ModelVersion docs page:
       https://docs.neptune.ai/api-reference/model-version
    """

    container_type = ContainerType.MODEL_VERSION

    def __init__(
        self,
        _id: str,
        backend: NeptuneBackend,
        op_processor: OperationProcessor,
        background_job: BackgroundJob,
        lock: threading.RLock,
        workspace: str,
        project_name: str,
        sys_id: str,
        project_id: str,
    ):
        super().__init__(
            _id,
            backend,
            op_processor,
            background_job,
            lock,
            project_id,
            project_name,
            workspace,
        )
        self._sys_id = sys_id

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api-reference/model-version#.stop"

    @property
    def _label(self) -> str:
        return self._sys_id

    def change_stage(self, stage: str):
        stage = ModelVersionStage(stage)

        if self._op_processor.mode == Mode.OFFLINE:
            raise NeptuneOfflineModeChangeStageException()

        self.wait()

        # We are sure that such attribute exists, because
        # SYSTEM_STAGE_ATTRIBUTE_PATH is set by default on ModelVersion creation
        attr = self.get_attribute(SYSTEM_STAGE_ATTRIBUTE_PATH)

        attr.process_assignment(
            value=stage.value,
            wait=True,
        )
