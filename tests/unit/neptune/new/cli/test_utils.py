#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
import uuid

from neptune.cli.utils import detect_async_dir
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.metadata_containers.structure_version import StructureVersion


def test__split_dir_name():
    # given
    random_id = UniqueId(str(uuid.uuid4()))

    assert detect_async_dir(f"{random_id}") == (ContainerType.RUN, random_id, StructureVersion.LEGACY)
    assert detect_async_dir(f"run__{random_id}") == (
        ContainerType.RUN,
        random_id,
        StructureVersion.CHILD_EXECUTION_DIRECTORIES,
    )
    assert detect_async_dir(f"model__{random_id}") == (
        ContainerType.MODEL,
        random_id,
        StructureVersion.CHILD_EXECUTION_DIRECTORIES,
    )
    assert detect_async_dir(f"project__{random_id}") == (
        ContainerType.PROJECT,
        random_id,
        StructureVersion.CHILD_EXECUTION_DIRECTORIES,
    )
    assert detect_async_dir(f"model_version__{random_id}") == (
        ContainerType.MODEL_VERSION,
        random_id,
        StructureVersion.CHILD_EXECUTION_DIRECTORIES,
    )
    assert detect_async_dir(f"run__{random_id}__1234__abcdefgh") == (
        ContainerType.RUN,
        random_id,
        StructureVersion.DIRECT_DIRECTORY,
    )
    assert detect_async_dir(f"project__{random_id}__1234__abcdefgh") == (
        ContainerType.PROJECT,
        random_id,
        StructureVersion.DIRECT_DIRECTORY,
    )
    assert detect_async_dir(f"model__{random_id}__1234__abcdefgh") == (
        ContainerType.MODEL,
        random_id,
        StructureVersion.DIRECT_DIRECTORY,
    )
    assert detect_async_dir(f"model_version__{random_id}__1234__abcdefgh") == (
        ContainerType.MODEL_VERSION,
        random_id,
        StructureVersion.DIRECT_DIRECTORY,
    )
