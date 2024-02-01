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
    prepare_v2_container,
)

AVAILABLE_CONTAINERS = [ContainerType.RUN, ContainerType.MODEL_VERSION, ContainerType.MODEL, ContainerType.PROJECT]


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_clean_v2_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=1, pid=1234, key="a1b2c3"
    )
    synced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=3, pid=1235, key="d4e5f6"
    )
    offline_containers = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=None, pid=1236, key="g7h8j9"
    )

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(unsynced_container.id)}__1234__a1b2c3"
    )
    assert os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(synced_container.id)}__1235__d4e5f6"
    )
    assert os.path.exists(
        tmp_path / OFFLINE_DIRECTORY / f"{container_type.create_dir_name(offline_containers.id)}__1236__g7h8j9"
    )

    # when
    ClearRunner.clear(backend=backend, path=tmp_path, force=True)

    # then
    assert not os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(unsynced_container.id)}__1234__a1b2c3"
    )
    assert not os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(synced_container.id)}__1235__d4e5f6"
    )
    assert not os.path.exists(
        tmp_path / OFFLINE_DIRECTORY / f"{container_type.create_dir_name(offline_containers.id)}__1236__g7h8j9"
    )

    # and
    offline_container_prefix = container_type.create_dir_name(offline_containers.id)
    unsynced_container_prefix = container_type.create_dir_name(unsynced_container.id)
    synced_container_prefix = container_type.create_dir_name(synced_container.id)

    # and
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / f'{synced_container_prefix}__1235__d4e5f6'}",
        "",
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)}",
        "",
        "Unsynchronized offline objects:",
        f"- offline/{offline_containers.id}",
        f"Deleted: {tmp_path / OFFLINE_DIRECTORY / f'{offline_container_prefix}__1236__g7h8j9'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / f'{unsynced_container_prefix}__1234__a1b2c3'}",
    ]


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_clean_v2_deleted_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=1, pid=1234, key="a1b2c3"
    )
    synced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=3, pid=1235, key="d4e5f6"
    )

    # and
    empty_get_container_impl = generate_get_metadata_container(registered_containers=[])

    # and
    mocker.patch.object(backend, "get_metadata_container", empty_get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(unsynced_container.id)}__1234__a1b2c3"
    )
    assert os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(synced_container.id)}__1235__d4e5f6"
    )
    # when
    ClearRunner.clear(backend=backend, path=tmp_path, force=True)

    # then
    assert not os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(unsynced_container.id)}__1234__a1b2c3"
    )
    assert not os.path.exists(
        tmp_path / ASYNC_DIRECTORY / f"{container_type.create_dir_name(synced_container.id)}__1235__d4e5f6"
    )

    # and
    unsynced_container_prefix = container_type.create_dir_name(unsynced_container.id)
    synced_container_prefix = container_type.create_dir_name(synced_container.id)

    # and
    captured = capsys.readouterr()
    assert set(captured.out.splitlines()) == {
        f"Can't fetch ContainerType.{container_type.name} {synced_container.id}. Skipping.",
        f"Can't fetch ContainerType.{container_type.name} {unsynced_container.id}. Skipping.",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / f'{synced_container_prefix}__1235__d4e5f6'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / f'{unsynced_container_prefix}__1234__a1b2c3'}",
    }


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_clean_v1_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)
    offline_containers = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=None)

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))
    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert os.path.exists(tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id))

    # when
    ClearRunner.clear(backend=backend, path=tmp_path, force=True)

    # then
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert not os.path.exists(tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id))

    # and
    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id) / 'exec-0'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id)}",
        "",
        "Unsynchronized objects:",
        f"- {get_qualified_name(unsynced_container)}",
        "",
        "Unsynchronized offline objects:",
        f"- offline/{offline_containers.id}",
        f"Deleted: {tmp_path / OFFLINE_DIRECTORY / container_type.create_dir_name(offline_containers.id)}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id) / 'exec-0'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id)}",
    ]


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_clean_v1_deleted_containers(tmp_path, mocker, capsys, backend, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)

    # and
    empty_get_container_impl = generate_get_metadata_container(registered_containers=[])

    # and
    mocker.patch.object(backend, "get_metadata_container", empty_get_container_impl)
    mocker.patch.object(Operation, "from_dict")

    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))

    # when
    ClearRunner.clear(backend=backend, path=tmp_path, force=True)

    # then
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id))
    assert not os.path.exists(tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id))

    # and
    captured = capsys.readouterr()
    assert set(captured.out.splitlines()) == {
        f"Can't fetch ContainerType.{container_type.name} {synced_container.id}. Skipping.",
        f"Can't fetch ContainerType.{container_type.name} {unsynced_container.id}. Skipping.",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id) / 'exec-0'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(synced_container.id)}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id) / 'exec-0'}",
        f"Deleted: {tmp_path / ASYNC_DIRECTORY / container_type.create_dir_name(unsynced_container.id)}",
    }


def test_clean_sync_directory(tmp_path, backend):
    # given
    sync_directory = tmp_path / SYNC_DIRECTORY
    sync_directory.mkdir(parents=True, exist_ok=True)

    assert os.path.exists(sync_directory)

    # when
    ClearRunner.clear(backend=backend, path=tmp_path)

    # then
    assert not os.path.exists(sync_directory)
