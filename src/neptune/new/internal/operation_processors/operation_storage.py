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
__all__ = ("OperationStorage",)

import abc
from datetime import datetime

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_DATA_DIRECTORY,
    OFFLINE_DIRECTORY,
    SYNC_DIRECTORY,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import UniqueId
from neptune.new.sync.utils import create_dir_name


class OperationStorage(abc.ABC):
    def __init__(self, container_id: UniqueId, container_type: ContainerType):
        self._data_path = self._init_data_path(container_id, container_type)

    @abc.abstractmethod
    def _init_data_path(self, container_id: UniqueId, container_type: ContainerType):
        raise NotImplementedError()

    @property
    def data_path(self):
        return self._data_path

    @property
    def upload_path(self):
        return f"{self.data_path}/upload_path"

    @staticmethod
    def _get_container_dir(type_dir: str, container_id: UniqueId, container_type: ContainerType):
        return f"{NEPTUNE_DATA_DIRECTORY}/{type_dir}/{create_dir_name(container_type, container_id)}"


class SyncOperationStorage(OperationStorage):
    def _init_data_path(self, container_id: UniqueId, container_type: ContainerType):
        now = datetime.now()
        container_dir = self._get_container_dir(SYNC_DIRECTORY, container_id, container_type)
        data_path = f"{container_dir}/exec-{now.timestamp()}-{now}"
        data_path = data_path.replace(" ", "_").replace(":", ".")
        return data_path


class AsyncOperationStorage(OperationStorage):
    def _init_data_path(self, container_id: UniqueId, container_type: ContainerType):
        now = datetime.now()
        container_dir = self._get_container_dir(ASYNC_DIRECTORY, container_id, container_type)
        data_path = f"{container_dir}/exec-{now.timestamp()}-{now}"
        data_path = data_path.replace(" ", "_").replace(":", ".")
        return data_path


class OfflineOperationStorage(OperationStorage):
    def _init_data_path(self, container_id: UniqueId, container_type: ContainerType):
        container_dir = self._get_container_dir(ASYNC_DIRECTORY, container_id, container_type)
        return container_dir
