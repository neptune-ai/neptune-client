#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

__all__ = ["SyncRunner"]

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import click

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
    OFFLINE_NAME_PREFIX,
)
from neptune.new.envs import NEPTUNE_SYNC_BATCH_TIMEOUT_ENV
from neptune.new.exceptions import (
    CannotSynchronizeOfflineRunsWithoutProject,
    NeptuneConnectionLostException,
)
from neptune.new.internal.backends.api_model import ApiExperiment, Project
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import QualifiedName, UniqueId
from neptune.new.internal.operation import Operation
from neptune.new.sync.abstract_backend_runner import AbstractBackendRunner
from neptune.new.sync.utils import (
    get_project,
    get_qualified_name,
    is_container_synced,
    iterate_containers,
    get_metadata_container,
    get_offline_dirs,
    split_dir_name,
    create_dir_name,
)

retries_timeout = int(os.getenv(NEPTUNE_SYNC_BATCH_TIMEOUT_ENV, "3600"))


class SyncRunner(AbstractBackendRunner):
    def sync_run(self, run_path: Path, run: ApiExperiment) -> None:
        qualified_run_name = get_qualified_name(run)
        click.echo("Synchronising {}".format(qualified_run_name))
        for execution_path in run_path.iterdir():
            self.sync_execution(
                execution_path=execution_path,
                container_id=run.id,
                container_type=run.type,
            )
        click.echo(
            f"Synchronization of {run.type.value} {qualified_run_name} completed."
        )

    def sync_execution(
        self,
        execution_path: Path,
        container_id: UniqueId,
        container_type: ContainerType,
    ) -> None:
        disk_queue = DiskQueue(
            dir_path=execution_path,
            to_dict=lambda x: x.to_dict(),
            from_dict=Operation.from_dict,
            lock=threading.RLock(),
        )
        while True:
            batch, version = disk_queue.get_batch(1000)
            if not batch:
                break

            start_time = time.monotonic()
            expected_count = len(batch)
            version_to_ack = version - expected_count
            while True:
                try:
                    processed_count, _ = self._backend.execute_operations(
                        container_id=container_id,
                        container_type=container_type,
                        operations=batch,
                    )
                    version_to_ack += processed_count
                    batch = batch[processed_count:]
                    disk_queue.ack(version)
                    if version_to_ack == version:
                        break
                except NeptuneConnectionLostException as ex:
                    if time.monotonic() - start_time > retries_timeout:
                        raise ex
                    click.echo(
                        "Experiencing connection interruptions. "
                        "Will try to reestablish communication with Neptune. "
                        f"Internal exception was: {ex.cause.__class__.__name__}",
                        sys.stderr,
                    )

    def sync_all_registered_containers(self, base_path: Path) -> None:
        async_path = base_path / ASYNC_DIRECTORY
        for container_type, unique_id, path in iterate_containers(async_path):
            if not is_container_synced(path):
                run = get_metadata_container(
                    backend=self._backend,
                    container_id=unique_id,
                    container_type=container_type,
                )
                if run:
                    self.sync_run(run_path=path, run=run)

    def sync_selected_registered_containers(
        self, base_path: Path, qualified_container_names: Sequence[QualifiedName]
    ) -> None:
        for name in qualified_container_names:
            run = get_metadata_container(
                backend=self._backend,
                container_id=name,
            )
            if run:
                run_path = (
                    base_path / ASYNC_DIRECTORY / f"{create_dir_name(run.type, run.id)}"
                )
                run_path_deprecated = base_path / ASYNC_DIRECTORY / f"{run.id}"
                if run_path.exists():
                    self.sync_run(run_path=run_path, run=run)
                elif run_path_deprecated.exists():
                    self.sync_run(run_path=run_path_deprecated, run=run)
                else:
                    click.echo(
                        "Warning: Run '{}' does not exist in location {}".format(
                            name, base_path
                        ),
                        file=sys.stderr,
                    )

    def _register_offline_run(
        self, project: Project, container_type: ContainerType
    ) -> Optional[ApiExperiment]:
        try:
            if container_type == ContainerType.RUN:
                return self._backend.create_run(project.id)
            else:
                raise ValueError("Only runs are supported in offline mode")
        except Exception as e:
            click.echo(
                "Exception occurred while trying to create a run "
                "on the Neptune server. Please try again later",
                file=sys.stderr,
            )
            logging.exception(e)
            return None

    @staticmethod
    def _move_offline_run(
        base_path: Path,
        offline_dir: str,
        server_id: UniqueId,
        server_type: ContainerType,
    ) -> None:
        online_dir = create_dir_name(container_type=server_type, container_id=server_id)
        # create async directory for run
        (base_path / ASYNC_DIRECTORY / online_dir).mkdir(parents=True)
        # mv offline directory inside async one
        (base_path / OFFLINE_DIRECTORY / offline_dir).rename(
            base_path / ASYNC_DIRECTORY / online_dir / "exec-0-offline"
        )

    def register_offline_runs(
        self, base_path: Path, project: Project, offline_dirs: Iterable[str]
    ) -> List[ApiExperiment]:
        result = []
        for offline_dir in offline_dirs:
            offline_path = base_path / OFFLINE_DIRECTORY / offline_dir
            if offline_path.is_dir():
                container_type, _ = split_dir_name(dir_name=offline_dir)
                run = self._register_offline_run(project, container_type=container_type)
                if run:
                    self._move_offline_run(
                        base_path=base_path,
                        offline_dir=offline_dir,
                        server_id=run.id,
                        server_type=run.type,
                    )
                    click.echo(
                        f"Offline run {offline_dir} registered as {get_qualified_name(run)}"
                    )
                    result.append(run)
            else:
                click.echo(
                    f"Offline run {offline_dir} not found on disk.",
                    err=True,
                )
        return result

    def sync_offline_runs(
        self,
        base_path: Path,
        project_name: Optional[QualifiedName],
        offline_dirs: Sequence[UniqueId],
    ):
        if offline_dirs:
            project = get_project(project_name, backend=self._backend)
            if not project:
                raise CannotSynchronizeOfflineRunsWithoutProject
            registered_runs = self.register_offline_runs(
                base_path, project, offline_dirs
            )
            offline_runs_names = [get_qualified_name(exp) for exp in registered_runs]
            self.sync_selected_registered_containers(base_path, offline_runs_names)

    def sync_selected_containers(
        self,
        base_path: Path,
        project_name: Optional[str],
        container_names: Sequence[str],
    ) -> None:
        non_offline_container_names = [
            QualifiedName(name)
            for name in container_names
            if not name.startswith(OFFLINE_NAME_PREFIX)
        ]
        self.sync_selected_registered_containers(base_path, non_offline_container_names)

        offline_dirs = [
            UniqueId(name[len(OFFLINE_NAME_PREFIX) :])
            for name in container_names
            if name.startswith(OFFLINE_NAME_PREFIX)
        ]
        self.sync_offline_runs(base_path, project_name, offline_dirs)

    def sync_all_containers(self, base_path: Path, project_name: Optional[str]) -> None:
        self.sync_all_registered_containers(base_path)

        offline_dirs = get_offline_dirs(base_path)
        self.sync_offline_runs(base_path, project_name, offline_dirs)
