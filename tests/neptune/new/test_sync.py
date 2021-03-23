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

# pylint: disable=redefined-outer-name

import os
import uuid
from random import randint

import pytest

import neptune.new.sync
from neptune.new.constants import OFFLINE_DIRECTORY
from neptune.new.exceptions import ProjectNotFound
from neptune.new.internal.backends.api_model import Project
from neptune.new.internal.containers.disk_queue import DiskQueue
from neptune.new.internal.operation import Operation
from neptune.new.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.new.sync import (
    ApiRun,
    get_project,
    get_qualified_name,
    sync_all_runs,
    sync_selected_runs,
    synchronization_status,
)


def a_run():
    return ApiRun(uuid.uuid4(), 'EXP-{}'.format(randint(42, 12342)), 'org', 'proj', False)


def prepare_runs(path):
    unsync_exp = a_run()
    sync_exp = a_run()
    registered_runs = (unsync_exp, sync_exp)

    execution_id = "exec-0"

    for exp in registered_runs:
        exp_path = path / "async" / str(exp.uuid) / execution_id
        exp_path.mkdir(parents=True)
        queue = DiskQueue(exp_path, lambda x: x, lambda x: x)
        queue.put('op-0')
        queue.put('op-1')

    SyncOffsetFile(path / "async" / str(unsync_exp.uuid) / execution_id / "last_ack_version").write(1)
    SyncOffsetFile(path / "async" / str(unsync_exp.uuid) / execution_id / "last_put_version").write(2)

    SyncOffsetFile(path / "async" / str(sync_exp.uuid) / execution_id / "last_ack_version").write(2)
    SyncOffsetFile(path / "async" / str(sync_exp.uuid) / execution_id / "last_put_version").write(2)

    def get_run_impl(run_id):
        for run in registered_runs:
            if run_id in (str(run.uuid), get_qualified_name(run)):
                return run

    return unsync_exp, sync_exp, get_run_impl


def prepare_offline_run(path):
    offline_exp_uuid = str(uuid.uuid4())
    offline_exp_path = path / OFFLINE_DIRECTORY / offline_exp_uuid
    offline_exp_path.mkdir(parents=True)

    queue = DiskQueue(offline_exp_path, lambda x: x, lambda x: x)
    queue.put('op-0')
    queue.put('op-1')
    SyncOffsetFile(path / OFFLINE_DIRECTORY / offline_exp_uuid / "last_put_version").write(2)

    return offline_exp_uuid


def test_list_runs(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_run_impl = prepare_runs(tmp_path)
    offline_exp_uuid = prepare_offline_run(tmp_path)

    # and
    mocker.patch.object(neptune.new.sync, 'get_run', get_run_impl)
    mocker.patch.object(Operation, 'from_dict')

    # when
    synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronized runs:\n- {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Unsynchronized runs:\n- {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Unsynchronized offline runs:\n- offline/{}'.format(offline_exp_uuid) in captured.out


def test_list_runs_when_no_run(tmp_path, capsys):
    (tmp_path / "async").mkdir()
    # when
    with pytest.raises(SystemExit):
        synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'There are no Neptune runs' in captured.out


def test_sync_all_runs(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_run_impl = prepare_runs(tmp_path)
    offline_exp_uuid = prepare_offline_run(tmp_path)
    registered_offline_run = a_run()

    # and
    mocker.patch.object(neptune.new.sync, 'get_run', get_run_impl)
    mocker.patch.object(neptune.new.sync, 'backend')
    mocker.patch.object(neptune.new.sync.backend, 'execute_operations')
    mocker.patch.object(neptune.new.sync.backend, 'get_project',
                        lambda _: Project(uuid.uuid4(), 'project', 'workspace'))
    mocker.patch.object(neptune.new.sync, 'register_offline_run', lambda _: registered_offline_run)
    mocker.patch.object(Operation, 'from_dict', lambda x: x)

    # when
    sync_all_runs(tmp_path, 'foo')

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert ('Offline run {} registered as {}'
            .format(offline_exp_uuid, get_qualified_name(registered_offline_run))) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronization of run {} completed.'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) not in captured.out

    # and
    # pylint: disable=no-member
    neptune.new.sync.backend.execute_operations.has_calls([
        mocker.call(unsync_exp.uuid, ['op-1']),
        mocker.call(registered_offline_run.uuid, ['op-1'])
    ], any_order=True)


def test_sync_selected_runs(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_run_impl = prepare_runs(tmp_path)
    offline_exp_uuid = prepare_offline_run(tmp_path)
    registered_offline_exp = a_run()

    def get_run_impl_(run_id: str):
        if run_id in (str(registered_offline_exp.uuid), get_qualified_name(registered_offline_exp)):
            return registered_offline_exp
        else:
            return get_run_impl(run_id)

    # and
    mocker.patch.object(neptune.new.sync, 'get_run', get_run_impl_)
    mocker.patch.object(neptune.new.sync, 'backend')
    mocker.patch.object(neptune.new.sync.backend, 'execute_operations')
    mocker.patch.object(neptune.new.sync.backend, 'get_project',
                        lambda _: Project(uuid.uuid4(), 'project', 'workspace'))
    mocker.patch.object(neptune.new.sync, 'register_offline_run', lambda _: registered_offline_exp)
    mocker.patch.object(Operation, 'from_dict', lambda x: x)

    # when
    sync_selected_runs(tmp_path, 'some-name', [get_qualified_name(sync_exp), 'offline/' + offline_exp_uuid])

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronization of run {} completed.'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(registered_offline_exp)) in captured.out
    assert 'Synchronization of run {} completed.'.format(get_qualified_name(registered_offline_exp)) \
           in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    # pylint: disable=no-member
    neptune.new.sync.backend.execute_operations \
        .assert_called_with(registered_offline_exp.uuid, ['op-0', 'op-1'])


def test_get_project_no_name_set(mocker):
    # given
    mocker.patch.object(os, 'getenv')
    os.getenv.return_value = None

    # expect
    assert get_project(None) is None


def test_get_project_project_not_found(mocker):
    # given
    mocker.patch.object(neptune.new.sync, 'backend')
    mocker.patch.object(neptune.new.sync.backend, 'get_project')
    neptune.new.sync.backend.get_project.side_effect = ProjectNotFound('foo')

    # expect
    assert get_project('foo') is None


def test_sync_non_existent_run(tmp_path, mocker, capsys):
    # given
    mocker.patch.object(neptune.new.sync, 'get_project')
    mocker.patch.object(neptune.new.sync, 'get_run')
    neptune.new.sync.get_run.return_value = a_run()

    # when
    sync_selected_runs(tmp_path, 'foo', ['bar'])

    # then
    captured = capsys.readouterr()
    assert "Warning: Run 'bar' does not exist in location" in captured.err
