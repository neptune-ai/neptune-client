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
import abc

from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import UniqueId


class OperationStorage(abc.ABC):
    def __init__(self, container_id: UniqueId, container_type: ContainerType):
        self._data_path = self.init_data_path(container_id, container_type)

    @abc.abstractmethod
    def init_data_path(self, container_id: UniqueId, container_type: ContainerType):
        raise NotImplementedError()

    @property
    def data_path(self):
        return self._data_path

    @property
    def upload_path(self):
        return f"{self.data_path}/upload_path"
