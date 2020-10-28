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

import pytest

import neptune.alpha.sync
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.alpha.internal.operation import Operation
from neptune.alpha.sync import partition_experiments, Experiment, get_qualified_name, list_experiments, \
    sync_all_experiments, sync_selected_experiments


def an_experiment():
    return Experiment(str(uuid.uuid4()), 'EXP-{}'.format(randint(42, 142)), 'org', 'proj')


@pytest.fixture
def experiment_factory():
    def f(tmp_path):
        unsync_exp = an_experiment()
        sync_exp = an_experiment()
        experiments = (unsync_exp, sync_exp)

        for exp in experiments:
            exp_path = tmp_path / exp.uuid
            exp_path.mkdir()
            logfile_path = exp_path / 'operations-0.log'
            with open(logfile_path, 'w') as logfile:
                logfile.write('{"version":0,"op":{}}{"version":1,"op":{}}')
                logfile.flush()

        sync_offset_file = SyncOffsetFile(tmp_path / unsync_exp.uuid)
        sync_offset_file.write(0)

        sync_offset_file = SyncOffsetFile(tmp_path / sync_exp.uuid)
        sync_offset_file.write(1)

        def get_experiment_impl(experiment_id):
            for exp in experiments:
                if experiment_id in (exp.uuid, get_qualified_name(exp)):
                    return exp

        return (unsync_exp, sync_exp, get_experiment_impl)
    return f


def test_list_experiments(experiment_factory, tmp_path, monkeypatch, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = experiment_factory(tmp_path)

    # and
    monkeypatch.setattr(neptune.alpha.sync, 'get_experiment', mocker.MagicMock(side_effect=get_experiment_impl))
    monkeypatch.setattr(Operation, 'from_dict', mocker.MagicMock())

    # when
    result = partition_experiments(tmp_path)
    list_experiments(tmp_path, *result)

    # then
    assert result[0] == [sync_exp]
    assert result[1] == [unsync_exp]

    # and
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronised experiments:\n- {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Unsynchronised experiments:\n- {}'.format(get_qualified_name(unsync_exp)) in captured.out

def test_sync_all_experiments(experiment_factory, tmp_path, monkeypatch, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = experiment_factory(tmp_path)

    # and
    execute_operations_mock = mocker.MagicMock()
    monkeypatch.setattr(neptune.alpha.sync, 'get_experiment', mocker.MagicMock(side_effect=get_experiment_impl))
    monkeypatch.setattr(neptune.alpha.sync, 'backend', mocker.MagicMock())
    monkeypatch.setattr(neptune.alpha.sync.backend, 'execute_operations', execute_operations_mock)
    monkeypatch.setattr(Operation, 'from_dict', lambda _: 'some-op')

    # when
    sync_all_experiments(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(unsync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) not in captured.out

    # and
    execute_operations_mock.assert_called_once_with(unsync_exp.uuid, ['some-op'])

def test_sync_selected_experiments(experiment_factory, tmp_path, monkeypatch, mocker, capsys):
    # given
    unsync_exp, sync_exp, get_experiment_impl = experiment_factory(tmp_path)

    # and
    execute_operations_mock = mocker.MagicMock()
    monkeypatch.setattr(neptune.alpha.sync, 'get_experiment', mocker.MagicMock(side_effect=get_experiment_impl))
    monkeypatch.setattr(neptune.alpha.sync, 'backend', mocker.MagicMock())
    monkeypatch.setattr(neptune.alpha.sync.backend, 'execute_operations', execute_operations_mock)
    monkeypatch.setattr(Operation, 'from_dict', lambda _: 'some-op')

    # when
    sync_selected_experiments(tmp_path, [get_qualified_name(sync_exp)])

    # then
    captured = capsys.readouterr()
    assert captured.err == ''
    assert 'Synchronising {}'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronization of experiment {} completed.'.format(get_qualified_name(sync_exp)) in captured.out
    assert 'Synchronising {}'.format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    execute_operations_mock.assert_not_called()
