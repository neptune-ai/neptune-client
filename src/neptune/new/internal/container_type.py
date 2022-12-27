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
__all__ = ["ContainerType"]

import enum

from neptune.new.internal.id_formats import UniqueId


class ContainerType(str, enum.Enum):
    RUN = "run"
    PROJECT = "project"
    MODEL = "model"
    MODEL_VERSION = "model_version"

    def to_api(self) -> str:
        if self == ContainerType.MODEL_VERSION:
            return "modelVersion"
        else:
            return self.value

    @staticmethod
    def from_api(api_type: str) -> "ContainerType":
        if api_type == "modelVersion":
            return ContainerType.MODEL_VERSION
        else:
            return ContainerType(api_type)

    def create_dir_name(self, container_id: UniqueId) -> str:
        return f"{self.value}__{container_id}"
