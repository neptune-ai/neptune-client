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

__all__ = [
    "get_metadata_container",
    "get_project",
    "get_qualified_name",
    "is_container_synced",
    "get_offline_dirs",
    "iterate_containers",
    "create_dir_name",
    "split_dir_name",
]

import logging
import os
import sys
import textwrap
import threading
from pathlib import Path
from typing import Optional, Iterator, Tuple, List, Union

import click

from neptune.new.constants import OFFLINE_DIRECTORY
from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.exceptions import (
    NeptuneException,
    ProjectNotFound,
    MetadataContainerNotFound,
)
from neptune.new.internal.backends.api_model import ApiExperiment, Project
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import QualifiedName, UniqueId
from neptune.new.internal.operation import Operation


def get_metadata_container(
    backend: NeptuneBackend,
    container_id: Union[UniqueId, QualifiedName],
    container_type: Optional[ContainerType] = None,
) -> Optional[ApiExperiment]:
    public_container_type = container_type or "object"
    try:
        return backend.get_metadata_container(
            container_id, expected_container_type=container_type
        )
    except MetadataContainerNotFound:
        click.echo(f"Can't fetch {public_container_type} {container_id}. Skipping.")
    except NeptuneException as e:
        click.echo(
            f"Exception while fetching {public_container_type} {container_id}. Skipping.",
            err=True,
        )
        logging.exception(e)

    return None


_project_name_missing_message = (
    "Project name not provided. Could not synchronize offline runs. "
    "To synchronize offline run, specify the project name with the --project flag "
    "or by setting the {} environment variable.".format(PROJECT_ENV_NAME)
)


def _project_not_found_message(project_name: QualifiedName) -> str:
    return (
        "Project {} not found. Could not synchronize offline runs. ".format(
            project_name
        )
        + "Please ensure you specified the correct project name with the --project flag "
        + "or with the {} environment variable, or contact Neptune for support.".format(
            PROJECT_ENV_NAME
        )
    )


def get_project(
    project_name_flag: Optional[QualifiedName], backend: NeptuneBackend
) -> Optional[Project]:
    project_name = project_name_flag or QualifiedName(os.getenv(PROJECT_ENV_NAME))
    if not project_name:
        click.echo(textwrap.fill(_project_name_missing_message), file=sys.stderr)
        return None
    try:
        return backend.get_project(project_name)
    except ProjectNotFound:
        click.echo(
            textwrap.fill(_project_not_found_message(project_name)), file=sys.stderr
        )
        return None


def get_qualified_name(run: ApiExperiment) -> QualifiedName:
    return QualifiedName("{}/{}/{}".format(run.workspace, run.project_name, run.sys_id))


def is_container_synced(run_path: Path) -> bool:
    return all(
        _is_execution_synced(execution_path) for execution_path in run_path.iterdir()
    )


def _is_execution_synced(execution_path: Path) -> bool:
    disk_queue = DiskQueue(
        execution_path,
        lambda x: x.to_dict(),
        Operation.from_dict,
        threading.RLock(),
    )
    return disk_queue.is_empty()


def create_dir_name(container_type: ContainerType, container_id: UniqueId) -> str:
    return f"{container_type.value}__{container_id}"


def split_dir_name(dir_name: str) -> Tuple[ContainerType, UniqueId]:
    parts = dir_name.split("__")
    if len(parts) == 2:
        return ContainerType(parts[0]), UniqueId(parts[1])
    elif len(parts) == 1:
        return ContainerType.RUN, UniqueId(dir_name)
    else:
        raise ValueError(f"Wrong dir format: {dir_name}")


def iterate_containers(
    base_path: Path,
) -> Iterator[Tuple[ContainerType, UniqueId, Path]]:
    if not base_path.is_dir():
        return

    for path in base_path.iterdir():
        container_type, unique_id = split_dir_name(dir_name=path.name)

        yield container_type, unique_id, path


def get_offline_dirs(base_path: Path) -> List[UniqueId]:
    result = []
    if not (base_path / OFFLINE_DIRECTORY).is_dir():
        return []
    for path_ in (base_path / OFFLINE_DIRECTORY).iterdir():
        dir_name = path_.name
        result.append(UniqueId(dir_name))
    return result
