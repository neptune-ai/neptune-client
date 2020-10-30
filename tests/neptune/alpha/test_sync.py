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

import uuid
from random import randint

import neptune.alpha.sync
from neptune.alpha.constants import OPERATIONS_DISK_QUEUE_PREFIX, OFFLINE_DIRECTORY
from neptune.alpha.internal.backends.api_model import Project
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.alpha.internal.operation import Operation
from neptune.alpha.sync import partition_experiments, Experiment, get_qualified_name, list_experiments, \
    sync_selected_experiments, get_offline_experiments_ids, sync_all_experiments


def an_experiment():
    return Experiment(str(uuid.uuid4()), 'EXP-{}'.format(randint(42, 12342)), 'org', 'proj')


def prepare_experiments(tmp_path):
    unsync_exp = an_experiment()
    sync_exp = an_experiment()
    registered_experiments = (unsync_exp, sync_exp)

    for exp in registered_experiments:
        exp_path = tmp_path / exp.uuid
        exp_path.mkdir()
        queue = DiskQueue(str(exp_path), OPERATIONS_DISK_QUEUE_PREFIX, lambda x: x, lambda x: x)
        queue.put({'version': 0, 'op': 'op-0'})
        queue.put({'version': 1, 'op': 'op-1'})

    sync_offset_file = SyncOffsetFile(tmp_path / unsync_exp.uuid)
    sync_offset_file.write(0)

    sync_offset_file = SyncOffsetFile(tmp_path / sync_exp.uuid)
    sync_offset_file.write(1)

    def get_experiment_impl(experiment_id):
        for exp in registered_experiments:
            if experiment_id in (exp.uuid, get_qualified_name(exp)):
                return exp

    offline_exp_uuid = str(uuid.uuid4())
    offline_exp_path = tmp_path / OFFLINE_DIRECTORY / offline_exp_uuid
    offline_exp_path.mkdir(parents=True)

    queue = DiskQueue(str(offline_exp_path), OPERATIONS_DISK_QUEUE_PREFIX, lambda x: x, lambda x: x)
    queue.put({'version': 0, 'op': 'op-0'})
    queue.put({'version': 1, 'op': 'op-1'})

    return unsync_exp, sync_exp, offline_exp_uuid, get_experiment_impl


def test_list_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, offline_exp_uuid, get_experiment_impl = prepare_experiments(tmp_path)

    # and
    mocker.patch.object(neptune.alpha.sync, 'get_experiment', get_experiment_impl)
    mocker.patch.object(Operation, 'from_dict')

    # when
    synced_experiments, unsynced_experiments = partition_experiments(tmp_path)
    offline_experiments_ids = get_offline_experiments_ids(tmp_path)
    list_experiments(tmp_path, synced_experiments, unsynced_experiments, offline_experiments_ids)

    # then
    assert synced_experiments == [sync_exp]
    assert unsynced_experiments == [unsync_exp]
    assert offline_experiments_ids == [offline_exp_uuid]

    # and
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronized experiments:\n- {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Unsynchronized experiments:\n- {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Unsynchronized offline experiments:\n- {}'.format(offline_exp_uuid) in captured.out


def test_sync_all_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, offline_exp_uuid, get_experiment_impl = prepare_experiments(tmp_path)
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
        mocker.call(uuid.UUID(unsync_exp.uuid), ['op-1']),
        mocker.call(uuid.UUID(registered_offline_experiment.uuid), ['op-1'])
    ], any_order=True)


def test_sync_selected_experiments(tmp_path, mocker, capsys):
    # given
    unsync_exp, sync_exp, offline_exp_uuid, get_experiment_impl = prepare_experiments(tmp_path)
    registered_offline_experiment = an_experiment()

    def get_experiment_impl_(experiment_id: str):
        if experiment_id in (registered_offline_experiment.uuid, get_qualified_name(registered_offline_experiment)):
            return registered_offline_experiment
        else:
            return get_experiment_impl(experiment_id)

    # and
    mocker.patch.object(neptune.alpha.sync, 'get_experiment', get_experiment_impl_)
    mocker.patch.object(neptune.alpha.sync, 'backend')
    mocker.patch.object(neptune.alpha.sync.backend, 'execute_operations')
    mocker.patch.object(neptune.alpha.sync.backend, 'get_project',
                        lambda _: Project(uuid.uuid4(), 'project', 'workspace'))
    mocker.patch.object(neptune.alpha.sync, 'register_offline_experiment', lambda _: registered_offline_experiment)
    mocker.patch.object(Operation, 'from_dict', lambda x: x)

    # when
    sync_selected_experiments(tmp_path, 'some-name', [get_qualified_name(sync_exp), offline_exp_uuid])

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(registered_offline_experiment)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(registered_offline_experiment)) \
           in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    # pylint: disable=no-member
    neptune.alpha.sync.backend.execute_operations \
        .assert_called_with(uuid.UUID(registered_offline_experiment.uuid), ['op-1'])
