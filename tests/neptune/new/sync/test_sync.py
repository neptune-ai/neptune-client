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

from unittest.mock import MagicMock

import pytest

from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import Operation
from neptune.new.sync import SyncRunner
from neptune.new.sync.utils import get_qualified_name
from tests.neptune.new.sync.utils import (
    generate_get_run_impl,
    prepare_offline_run,
    prepare_projects,
    prepare_run,
)
from tests.neptune.new.utils.api_experiments_factory import api_run


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.fixture(name="sync_runner")
def sync_runner_fixture(backend):
    return SyncRunner(backend=backend)


def test_sync_all_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    unsync_proj, sync_proj, _ = prepare_projects(tmp_path)
    unsync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=1,
    )
    sync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=2,
    )
    get_run_impl = generate_get_run_impl(
        registered_experiments=(unsync_proj, sync_proj, unsync_exp, sync_exp)
    )
    offline_run = prepare_offline_run(tmp_path)

    # and
    mocker.patch.object(backend, "get_run", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_run",
        lambda project, container_type: (offline_run, True),
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)
    backend.execute_operations.return_value = (1, [])

    # when
    sync_runner.sync_all_runs(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Offline run {} registered as {}".format(
            f"run__{offline_run.id}", get_qualified_name(offline_run)
        )
    ) in captured.out
    assert "Synchronising {}".format(get_qualified_name(unsync_exp)) in captured.out
    assert "Synchronising {}".format(get_qualified_name(unsync_proj)) in captured.out
    assert (
        "Synchronization of run {} completed.".format(get_qualified_name(unsync_exp))
        in captured.out
    )
    assert (
        "Synchronization of project {} completed.".format(
            get_qualified_name(unsync_proj)
        )
        in captured.out
    )
    assert "Synchronising {}".format(get_qualified_name(sync_exp)) not in captured.out
    assert "Synchronising {}".format(get_qualified_name(sync_proj)) not in captured.out

    # and
    backend.execute_operations.has_calls(
        [
            mocker.call(unsync_exp.id, ContainerType.RUN, ["op-1"]),
            mocker.call(offline_run.id, ContainerType.RUN, ["op-1"]),
            mocker.call(unsync_proj.id, ContainerType.PROJECT, ["op-proj-1"]),
        ],
        any_order=True,
    )


def test_sync_selected_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    unsync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=1,
    )
    sync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=2,
    )
    offline_run = prepare_offline_run(tmp_path)
    get_run_impl = generate_get_run_impl(
        registered_experiments=[unsync_exp, sync_exp, offline_run]
    )

    # and
    mocker.patch.object(backend, "get_run", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_run",
        lambda project, container_type: (offline_run, True),
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)
    backend.execute_operations.return_value = (2, [])

    # when
    sync_runner.sync_selected_runs(
        tmp_path,
        "some-name",
        [get_qualified_name(sync_exp), "offline/" + offline_run.id],
    )

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Synchronising {}".format(get_qualified_name(sync_exp)) in captured.out
    assert (
        "Synchronization of run {} completed.".format(get_qualified_name(sync_exp))
        in captured.out
    )
    assert "Synchronising {}".format(get_qualified_name(offline_run)) in captured.out
    assert (
        "Synchronization of run {} completed.".format(get_qualified_name(offline_run))
        in captured.out
    )
    assert "Synchronising {}".format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    backend.execute_operations.assert_called_with(
        offline_run.id, ContainerType.RUN, operations=["op-0", "op-1"]
    )


def test_sync_non_existent_run(tmp_path, capsys, backend, sync_runner):
    # given
    backend.get_run.return_value = api_run()

    # when
    sync_runner.sync_selected_runs(tmp_path, "foo", ["bar"])

    # then
    captured = capsys.readouterr()
    assert "Warning: Run 'bar' does not exist in location" in captured.err
