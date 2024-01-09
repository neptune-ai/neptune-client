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
import itertools
import os
from unittest.mock import MagicMock

import pytest

from neptune.cli.clear import ClearRunner
from neptune.cli.utils import get_qualified_name
from neptune.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
    SYNC_DIRECTORY,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import Operation
from tests.unit.neptune.new.cli.utils import (
    generate_get_metadata_container,
    prepare_v1_container,
)


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.fixture(name="clear_runner")
def status_runner_fixture(backend):
    return ClearRunner(backend=backend)


@pytest.mark.parametrize("container_type", list(ContainerType))
def test_clean_containers(tmp_path, mocker, capsys, backend, clear_runner, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)
    offline_containers = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=None)
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))
    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert os.path.exists(tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id))

    # when
    clear_runner.clear(tmp_path, force=True)

    # then
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert not os.path.exists(tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id))

    # and
    captured = capsys.readouterr()
    expected_out_lines = [
        "",
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)}",
        "",
        "Unsynchronized offline objects:",
        f"- offline/{container_type.create_dir_name(offline_containers.id)}",
        f"Deleted: {tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id)}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id)}",
    ]
    for captured, expected in itertools.zip_longest(captured.out.splitlines(), expected_out_lines):
        assert captured.endswith(expected)


@pytest.mark.parametrize("container_type", list(ContainerType))
def test_clean_deleted_containers(tmp_path, mocker, capsys, backend, clear_runner, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)
    empty_get_container_impl = generate_get_metadata_container(registered_containers=[])

    # and
    mocker.patch.object(backend, "get_metadata_container", empty_get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))

    # when
    clear_runner.clear(tmp_path, force=True)

    # then
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))

    # and
    captured = capsys.readouterr()
    expected_out_lines = [
        f"Can't fetch ContainerType.{container_type.name} {synced_container.id}. Skipping.",
        f"Can't fetch ContainerType.{container_type.name} {unsynced_container.id}. Skipping.",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id)}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id)}",
    ]

    # sort lines that will be compared by their postfix so we get consistent test execution (prefix might change)
    captured_out_lines = sorted(captured.out.splitlines(), key=lambda x: x[::-1])
    expected_out_lines = sorted(expected_out_lines, key=lambda x: x[::-1])
    for captured, expected in itertools.zip_longest(captured_out_lines, expected_out_lines):
        assert captured.endswith(expected)


def test_clean_sync_directory(tmp_path, clear_runner):
    # given
    sync_directory = tmp_path / SYNC_DIRECTORY
    sync_directory.mkdir(parents=True, exist_ok=True)

    assert os.path.exists(sync_directory)

    # when
    clear_runner.clear(tmp_path)

    # then
    assert not os.path.exists(sync_directory)
