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
from functools import partial
from unittest.mock import MagicMock

import pytest

from neptune.cli.status import StatusRunner
from neptune.cli.utils import get_qualified_name
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import Operation
from tests.unit.neptune.new.cli.utils import (
    generate_get_metadata_container,
    prepare_v1_container,
)
from tests.unit.neptune.new.utils.logging import format_log


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.fixture(name="status_runner")
def status_runner_fixture(backend):
    return StatusRunner(backend=backend)


@pytest.mark.parametrize("container_type", list(ContainerType))
def test_list_containers(tmp_path, mocker, capsys, backend, status_runner, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)
    _log = partial(format_log, "INFO")

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        _log("Unsynchronized objects:"),
        _log(f"- {get_qualified_name(unsynced_container)}"),
        _log(""),
        # this one without formatting as it got splitted by new line
        "Please run with the `neptune sync --help` to see example commands.",
    ]


def test_list_offline_runs(tmp_path, mocker, capsys, status_runner):
    # given
    offline_run = prepare_v1_container(
        container_type=ContainerType.RUN,
        path=tmp_path,
        last_ack_version=None,
    )

    # and
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)
    _log = partial(format_log, "INFO")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert set(captured.out.splitlines()).issuperset(
        set(
            [
                _log("Unsynchronized offline objects:"),
                _log(f"- offline/run__{offline_run.id}"),
                _log(""),
            ]
        )
    )


def test_list_trashed_containers(tmp_path, mocker, capsys, backend, status_runner):
    # given
    unsynced_container = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1, trashed=True
    )
    synced_container = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3, trashed=True
    )
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)
    _log = partial(format_log, "INFO")

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        _log("Unsynchronized objects:"),
        _log(f"- {get_qualified_name(unsynced_container)} (Trashed)"),
        _log(""),
        # this one without formatting as it got splitted by new line
        "Please run with the `neptune sync --help` to see example commands.",
    ]


def test_list_runs_when_no_run(tmp_path, capsys, status_runner):
    (tmp_path / "async").mkdir()
    # when
    with pytest.raises(SystemExit):
        status_runner.synchronization_status(tmp_path)
    _log = partial(format_log, "INFO")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert _log("There are no Neptune objects") in captured.out
