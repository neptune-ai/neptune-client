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
from unittest.mock import MagicMock

import pytest

from neptune.new.internal.operation import Operation
from neptune.new.sync import StatusRunner
from neptune.new.sync.utils import get_qualified_name
from tests.neptune.new.sync.utils import (
    prepare_offline_run,
    prepare_projects,
    prepare_run,
    generate_get_run_impl,
)


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.fixture(name="status_runner")
def status_runner_fixture(backend):
    return StatusRunner(backend=backend)


def test_list_projects(tmp_path, mocker, capsys, backend, status_runner):
    """TODO: we're mentioning projects as runs, will be improved with ModelRegistry"""
    # given
    unsync_proj, sync_proj, get_exp_impl = prepare_projects(tmp_path)
    offline_exp = prepare_offline_run(tmp_path)

    # and
    mocker.patch.object(backend, "get_run", get_exp_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Synchronized runs:\n- {}".format(get_qualified_name(sync_proj)) in captured.out
    )
    assert (
        "Unsynchronized runs:\n- {}".format(get_qualified_name(unsync_proj))
        in captured.out
    )
    assert (
        "Unsynchronized offline runs:\n- offline/{}".format(offline_exp.id)
        in captured.out
    )


def test_list_runs(tmp_path, mocker, capsys, backend, status_runner):
    # given
    unsync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=1,
    )
    sync_exp = prepare_run(
        path=tmp_path,
        last_ack_version=2,
    )
    get_run_impl = generate_get_run_impl(registered_experiments=[unsync_exp, sync_exp])
    offline_exp = prepare_offline_run(tmp_path)

    # and
    mocker.patch.object(backend, "get_run", get_run_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Synchronized runs:\n- {}".format(get_qualified_name(sync_exp)) in captured.out
    )
    assert (
        "Unsynchronized runs:\n- {}".format(get_qualified_name(unsync_exp))
        in captured.out
    )
    assert (
        "Unsynchronized offline runs:\n- offline/{}".format(offline_exp.id)
        in captured.out
    )


def test_list_runs_when_no_run(tmp_path, capsys, status_runner):
    (tmp_path / "async").mkdir()
    # when
    with pytest.raises(SystemExit):
        status_runner.synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "There are no Neptune runs" in captured.out
