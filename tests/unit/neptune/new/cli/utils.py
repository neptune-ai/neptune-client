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

import threading
from pathlib import Path
from typing import Optional

from neptune.cli.utils import get_qualified_name
from neptune.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.core.components.queue.sync_offset_file import SyncOffsetFile
from neptune.exceptions import MetadataContainerNotFound
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.container_type import ContainerType
from tests.unit.neptune.new.utils.api_experiments_factory import (
    api_metadata_container,
    api_run,
)


def generate_get_metadata_container(registered_containers):
    def get_metadata_container(container_id, expected_container_type: ContainerType):
        """This function will return run as well as projects. Will be cleaned in ModelRegistry"""
        for exp in registered_containers:
            if container_id in (str(exp.id), get_qualified_name(exp)):
                return exp

        raise MetadataContainerNotFound.of_container_type(
            container_type=expected_container_type, container_id=container_id
        )

    return get_metadata_container


def execute_operations(container_id, container_type, operations, operation_storage):
    return len(operations), []


def _prepare_disk_queue(*, exp_path, last_ack_version):
    exp_path.mkdir(parents=True)
    queue = DiskQueue(
        data_path=exp_path,
        to_dict=lambda x: x,
        from_dict=lambda x: x,
        lock=threading.RLock(),
    )
    queue.put("op-0")
    queue.put("op-1")
    queue.put("op-2")

    SyncOffsetFile(exp_path / "last_put_version").write(3)
    if last_ack_version is not None:
        SyncOffsetFile(exp_path / "last_ack_version").write(last_ack_version)


def prepare_v2_container(
    *,
    container_type: ContainerType,
    path: Path,
    pid: int,
    key: str,
    last_ack_version: Optional[int],
    trashed: Optional[bool] = False,
) -> ApiExperiment:
    container = api_metadata_container(container_type, trashed=trashed)

    exec_path = f"{container_type.create_dir_name(container.id)}__{pid}__{key}"
    directory = OFFLINE_DIRECTORY if last_ack_version is None else ASYNC_DIRECTORY
    experiment_path = path / directory / exec_path

    _prepare_disk_queue(exp_path=experiment_path, last_ack_version=last_ack_version)

    return container


def prepare_v1_container(
    *, container_type: ContainerType, path: Path, last_ack_version: Optional[int], trashed: Optional[bool] = False
) -> ApiExperiment:
    is_offline = last_ack_version is None

    container = api_metadata_container(container_type, trashed=trashed)

    if is_offline:
        exp_path = path / OFFLINE_DIRECTORY / f"{container.type.value}__{container.id}"
    else:
        execution_id = "exec-0"
        exp_path = path / ASYNC_DIRECTORY / f"{container.type.value}__{container.id}" / execution_id

    _prepare_disk_queue(
        exp_path=exp_path,
        last_ack_version=last_ack_version,
    )

    return container


def prepare_v0_run(*, path: Path, last_ack_version: Optional[int]):
    is_offline = last_ack_version is None

    run = api_run()

    if is_offline:
        exp_path = path / OFFLINE_DIRECTORY / run.id
    else:
        execution_id = "exec-0"
        exp_path = path / ASYNC_DIRECTORY / run.id / execution_id
    _prepare_disk_queue(
        exp_path=exp_path,
        last_ack_version=last_ack_version,
    )
    return run
