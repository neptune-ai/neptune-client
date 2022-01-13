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

from neptune.new.constants import OFFLINE_DIRECTORY, ASYNC_DIRECTORY
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.new.sync.utils import get_qualified_name
from tests.neptune.new.utils.api_experiments_factory import api_project, api_run


def generate_get_run_impl(registered_experiments):
    def get_run_impl(run_id):
        """This function will return run as well as projects. Will be cleaned in ModelRegistry"""
        for exp in registered_experiments:
            if run_id in (str(exp.id), get_qualified_name(exp)):
                return exp

    return get_run_impl


def prepare_projects(path):
    unsync_project = api_project()
    sync_project = api_project()
    registered_projects = (unsync_project, sync_project)

    execution_id = "exec-0"

    for project in registered_projects:
        project_path = path / "async" / str(project.id) / execution_id
        project_path.mkdir(parents=True)
        queue = DiskQueue(
            dir_path=project_path,
            to_dict=lambda x: x,
            from_dict=lambda x: x,
            lock=threading.RLock(),
        )
        queue.put("op-proj-0")
        queue.put("op-proj-1")

    SyncOffsetFile(
        path / "async" / str(unsync_project.id) / execution_id / "last_ack_version"
    ).write(1)
    SyncOffsetFile(
        path / "async" / str(unsync_project.id) / execution_id / "last_put_version"
    ).write(2)

    SyncOffsetFile(
        path / "async" / str(sync_project.id) / execution_id / "last_ack_version"
    ).write(2)
    SyncOffsetFile(
        path / "async" / str(sync_project.id) / execution_id / "last_put_version"
    ).write(2)

    return unsync_project, sync_project, generate_get_run_impl(registered_projects)


def _prepare_disk_queue(*, exp_path, last_ack_version):
    exp_path.mkdir(parents=True)
    queue = DiskQueue(
        dir_path=exp_path,
        to_dict=lambda x: x,
        from_dict=lambda x: x,
        lock=threading.RLock(),
    )
    queue.put("op-0")
    queue.put("op-1")

    SyncOffsetFile(exp_path / "last_put_version").write(2)
    if last_ack_version is not None:
        SyncOffsetFile(exp_path / "last_ack_version").write(last_ack_version)


def prepare_run(path, last_ack_version):
    exp = api_run()

    execution_id = "exec-0"

    exp_path = path / ASYNC_DIRECTORY / f"{exp.type.value}__{exp.id}" / execution_id
    _prepare_disk_queue(
        exp_path=exp_path,
        last_ack_version=last_ack_version,
    )

    return exp


def prepare_offline_run(path):
    exp = api_run()

    offline_exp_path = path / OFFLINE_DIRECTORY / f"run__{exp.id}"
    _prepare_disk_queue(
        exp_path=offline_exp_path,
        last_ack_version=None,
    )

    return exp
