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

from neptune.new.metadata_containers import MetadataContainer
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation_processors.operation_processor import (
    OperationProcessor,
)
from neptune.new.metadata_containers.model_versions_table import ModelVersionsTable


class Model(MetadataContainer):
    """A class for managing a Neptune model and retrieving information from it.

    You may also want to check `Model docs page`_.

    .. _Model docs page:
       https://docs.neptune.ai/api-reference/model
    """

    container_type = ContainerType.MODEL

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
        return "https://docs.neptune.ai/api-reference/model#.stop"

    @property
    def _label(self) -> str:
        return self._sys_id

    def fetch_model_versions_table(self) -> ModelVersionsTable:
        """TODO: NPT-11349"""
        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id, parent_id=self._id, types=[self.container_type]
        )

        return ModelVersionsTable(backend=self._backend, entries=leaderboard_entries)
