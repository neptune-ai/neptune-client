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

import neptune.alpha.sync
from neptune.alpha.constants import OFFLINE_DIRECTORY
from neptune.alpha.exceptions import ProjectNotFound
from neptune.alpha.internal.backends.api_model import Project
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.operation import Operation
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.alpha.sync import Experiment, get_qualified_name, \
    sync_selected_experiments, sync_all_experiments, synchronization_status, get_project


def an_experiment():
    return Experiment(uuid.uuid4(), 'EXP-{}'.format(randint(42, 12342)), 'org', 'proj')


def prepare_experiments(path):
    unsync_exp = an_experiment()
    sync_exp = an_experiment()
    registered_experiments = (unsync_exp, sync_exp)

    execution_id = "exec-0"

    for exp in registered_experiments:
        exp_path = path / "async" / str(exp.uuid) / execution_id
        exp_path.mkdir(parents=True)
        queue = DiskQueue(exp_path, lambda x, _: x, lambda x: x)
        queue.put('op-0')
        queue.put('op-1')

    SyncOffsetFile(path / "async" / str(unsync_exp.uuid) / execution_id / "last_ack_version").write(1)
    SyncOffsetFile(path / "async" / str(unsync_exp.uuid) / execution_id / "last_put_version").write(2)

    SyncOffsetFile(path / "async" / str(sync_exp.uuid) / execution_id / "last_ack_version").write(2)
    SyncOffsetFile(path / "async" / str(sync_exp.uuid) / execution_id / "last_put_version").write(2)

    def get_experiment_impl(experiment_id):
        for exp in registered_experiments:
            if experiment_id in (str(exp.uuid), get_qualified_name(exp)):
                return exp

    return unsync_exp, sync_exp, get_experiment_impl


def prepare_offline_experiment(path):
    offline_exp_uuid = str(uuid.uuid4())
    offline_exp_path = path / OFFLINE_DIRECTORY / offline_exp_uuid
    offline_exp_path.mkdir(parents=True)

    queue = DiskQueue(offline_exp_path, lambda x, _: x, lambda x: x)
    queue.put('op-0')
    queue.put('op-1')
    SyncOffsetFile(path / OFFLINE_DIRECTORY / offline_exp_uuid / "last_put_version").write(2)

    return offline_exp_uuid


def test_list_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = prepare_experiments(tmp_path)
    offline_exp_uuid = prepare_offline_experiment(tmp_path)

    # and
    mocker.patch.object(neptune.alpha.sync, 'get_experiment', get_experiment_impl)
    mocker.patch.object(Operation, 'from_dict')

    # when
    synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronized experiments:\n- {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Unsynchronized experiments:\n- {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Unsynchronized offline experiments:\n- offline/{}'.format(offline_exp_uuid) in captured.out


def test_list_experiments_when_no_experiment(tmp_path, capsys):
    (tmp_path / "async").mkdir()
    # when
    with pytest.raises(SystemExit):
        synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'There are no Neptune experiments' in captured.out


def test_sync_all_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = prepare_experiments(tmp_path)
    offline_exp_uuid = prepare_offline_experiment(tmp_path)
    registered_offline_experiment = an_experiment()

    # and
    mocker.patch.object(neptune.alpha.sync, 'get_experiment', get_experiment_impl)
    mocker.patch.object(neptune.alpha.sync, 'backend')
    mocker.patch.object(neptune.alpha.sync.backend, 'execute_operations')
    mocker.patch.object(neptune.alpha.sync.backend, 'get_project',
                        lambda _: Project(uuid.uuid4(), 'project', 'workspace'))
    mocker.patch.object(neptune.alpha.sync, 'register_offline_experiment', lambda _: registered_offline_experiment)
    mocker.patch.object(Operation, 'from_dict', lambda x: x)

    # when
    sync_all_experiments(tmp_path, 'foo')

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert ('Offline experiment {} registered as {}'
            .format(offline_exp_uuid, get_qualified_name(registered_offline_experiment))) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) not in captured.out

    # and
    # pylint: disable=no-member
    neptune.alpha.sync.backend.execute_operations.has_calls([
        mocker.call(unsync_exp.uuid, ['op-1']),
        mocker.call(registered_offline_experiment.uuid, ['op-1'])
    ], any_order=True)


def test_sync_selected_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = prepare_experiments(tmp_path)
    offline_exp_uuid = prepare_offline_experiment(tmp_path)
    registered_offline_exp = an_experiment()

    def get_experiment_impl_(experiment_id: str):
        if experiment_id in (str(registered_offline_exp.uuid), get_qualified_name(registered_offline_exp)):
            return registered_offline_exp
        else:
            return get_experiment_impl(experiment_id)

    # and
    mocker.patch.object(neptune.alpha.sync, 'get_experiment', get_experiment_impl_)
    mocker.patch.object(neptune.alpha.sync, 'backend')
    mocker.patch.object(neptune.alpha.sync.backend, 'execute_operations')
    mocker.patch.object(neptune.alpha.sync.backend, 'get_project',
                        lambda _: Project(uuid.uuid4(), 'project', 'workspace'))
    mocker.patch.object(neptune.alpha.sync, 'register_offline_experiment', lambda _: registered_offline_exp)
    mocker.patch.object(Operation, 'from_dict', lambda x: x)

    # when
    sync_selected_experiments(tmp_path, 'some-name', [get_qualified_name(sync_exp), 'offline/' + offline_exp_uuid])

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(registered_offline_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(registered_offline_exp)) \
           in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    # pylint: disable=no-member
    neptune.alpha.sync.backend.execute_operations \
        .assert_called_with(registered_offline_exp.uuid, ['op-0', 'op-1'])


def test_get_project_no_name_set(mocker):
    # given
    mocker.patch.object(os, 'getenv')
    os.getenv.return_value = None

    # expect
    assert get_project(None) is None


def test_get_project_project_not_found(mocker):
    # given
    mocker.patch.object(neptune.alpha.sync, 'backend')
    mocker.patch.object(neptune.alpha.sync.backend, 'get_project')
    neptune.alpha.sync.backend.get_project.side_effect = ProjectNotFound('foo')

    # expect
    assert get_project('foo') is None


def test_sync_non_existent_experiment(tmp_path, mocker, capsys):
    # given
    mocker.patch.object(neptune.alpha.sync, 'get_project')
    mocker.patch.object(neptune.alpha.sync, 'get_experiment')
    neptune.alpha.sync.get_experiment.return_value = an_experiment()

    # when
    sync_selected_experiments(tmp_path, 'foo', ['bar'])

    # then
    captured = capsys.readouterr()
    assert "Warning: Experiment 'bar' does not exist in location" in captured.err
