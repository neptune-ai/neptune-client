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

__all__ = ["ClearRunner"]

import shutil
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Sequence,
)

import click

from neptune.cli.collect import collect_containers
from neptune.cli.status import StatusRunner
from neptune.constants import SYNC_DIRECTORY
from neptune.internal.utils.logger import get_logger

if TYPE_CHECKING:
    from neptune.cli.containers import (
        AsyncContainer,
        Container,
        OfflineContainer,
    )
    from neptune.internal.backends.neptune_backend import NeptuneBackend

logger = get_logger(with_prefix=False)


class ClearRunner:
    @staticmethod
    def clear(*, backend: "NeptuneBackend", path: Path, force: bool = False, clear_eventual: bool = True) -> None:
        containers = collect_containers(path=path, backend=backend)

        remove_sync_containers(path=path)
        remove_containers(containers=containers.not_found_containers)
        remove_containers(
            containers=filter_containers(a=containers.synced_containers, b=containers.not_found_containers)
        )

        if clear_eventual and (containers.offline_containers or containers.unsynced_containers):
            log_junk_metadata(
                offline_containers=containers.offline_containers, unsynced_containers=containers.unsynced_containers
            )

            if force or click.confirm("\nDo you want to delete the listed metadata?"):
                remove_containers(containers=containers.offline_containers)
                remove_containers(containers=containers.unsynced_containers)


def filter_containers(*, a: Sequence["Container"], b: Sequence["Container"]) -> Sequence["Container"]:
    b_ids = {container.container_id for container in b}
    return [container for container in a if container.container_id not in b_ids]


def remove_sync_containers(*, path: Path) -> None:
    """
    Function can remove SYNC_DIRECTORY safely, Neptune client only stores files to upload in this location.
    """
    shutil.rmtree(path / SYNC_DIRECTORY, ignore_errors=True)


def log_junk_metadata(
    *, offline_containers: Sequence["OfflineContainer"], unsynced_containers: Sequence["AsyncContainer"]
) -> None:
    if unsynced_containers:
        logger.info("")
        StatusRunner.log_unsync_objects(unsynced_containers=unsynced_containers)

    if offline_containers:
        logger.info("")
        StatusRunner.log_offline_objects(offline_containers=offline_containers, info=False)


def remove_containers(*, containers: Sequence["Container"]) -> None:
    for container in containers:
        container.clear()
