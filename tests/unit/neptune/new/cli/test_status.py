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

from neptune.cli.status import StatusRunner
from neptune.cli.utils import get_qualified_name
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import Operation
from tests.unit.neptune.new.cli.utils import (
    generate_get_metadata_container,
    prepare_v1_container,
    prepare_v2_container,
)

AVAILABLE_CONTAINERS = [ContainerType.RUN, ContainerType.MODEL_VERSION, ContainerType.MODEL, ContainerType.PROJECT]


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_list_v2_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=1, pid=2501, key="a1b2c3"
    )
    synced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=3, pid=2502, key="d4e5f6"
    )

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)}",
        "",
        "Please run with the `neptune sync --help` to see example commands.",
    ]


def test_list_offline_v2_runs(tmp_path, mocker, capsys, backend):
    # given
    offline_run = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None, pid=2501, key="a1b2c3"
    )

    # and
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert f"Unsynchronized offline objects:\n- offline/{offline_run.id}" in captured.out


def test_list_trashed_v2_containers(tmp_path, mocker, capsys, backend):
    # given
    unsynced_container = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1, trashed=True, pid=2501, key="a1b2c3"
    )
    synced_container = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3, trashed=True, pid=2502, key="d4e5f6"
    )

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)} (Trashed)",
        "",
        "Please run with the `neptune sync --help` to see example commands.",
    ]


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_list_v1_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)}",
        "",
        "Please run with the `neptune sync --help` to see example commands.",
    ]


def test_list_offline_v1_runs(tmp_path, mocker, capsys, backend):
    # given
    offline_run = prepare_v1_container(
        container_type=ContainerType.RUN,
        path=tmp_path,
        last_ack_version=None,
    )

    # and
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Unsynchronized offline objects:\n- offline/{}".format(offline_run.id) in captured.out


def test_list_trashed_v1_containers(tmp_path, mocker, capsys, backend):
    # given
    unsynced_container = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1, trashed=True
    )
    synced_container = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3, trashed=True
    )

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    # when
    StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)} (Trashed)",
        "",
        "Please run with the `neptune sync --help` to see example commands.",
    ]


def test_list_runs_when_no_run(tmp_path, capsys, backend):
    (tmp_path / "async").mkdir()
    # when
    with pytest.raises(SystemExit):
        StatusRunner.status(backend=backend, path=tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "There are no Neptune objects" in captured.out
