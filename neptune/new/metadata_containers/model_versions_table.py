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
import logging
from typing import List

from neptune.new.metadata_containers.metadata_containers_table import (
    MetadataContainersTable,
    MetadataContainersTableEntry,
)
from neptune.new.internal.backends.api_model import (
    AttributeWithProperties,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.container_type import ContainerType

logger = logging.getLogger(__name__)


class ModelVersionsTableEntry(MetadataContainersTableEntry):
    def __init__(
        self,
        backend: NeptuneBackend,
        _id: str,
        attributes: List[AttributeWithProperties],
    ):
        super().__init__(
            backend=backend,
            container_type=ContainerType.MODEL_VERSION,
            _id=_id,
            attributes=attributes,
        )


class ModelVersionsTable(MetadataContainersTable):
    table_entry_cls = ModelVersionsTableEntry

    def to_model_versions(self):
        return self.to_table_entries()
