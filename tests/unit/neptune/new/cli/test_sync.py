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

import mock
import pytest

from neptune.cli.sync import SyncRunner
from neptune.cli.utils import get_qualified_name
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import Operation
from tests.unit.neptune.new.cli.utils import (
    execute_operations,
    generate_get_metadata_container,
    prepare_v0_run,
    prepare_v1_container,
    prepare_v2_container,
)

AVAILABLE_CONTAINERS = [ContainerType.RUN, ContainerType.MODEL_VERSION, ContainerType.MODEL, ContainerType.PROJECT]


@pytest.fixture(name="backend")
def backend_fixture():
    backend = MagicMock()
    backend.execute_operations.side_effect = execute_operations
    return backend


@pytest.fixture(name="sync_runner")
def sync_runner_fixture(backend):
    return SyncRunner(backend=backend)


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_sync_all_v2_containers(tmp_path, mocker, capsys, backend, sync_runner, container_type):
    # given
    unsynced_container = prepare_v2_container(
        container_type=container_type,
        path=tmp_path,
        last_ack_version=1,
        pid=2501,
        random_key="a1b2c3",
    )
    synced_container = prepare_v2_container(
        container_type=container_type, path=tmp_path, last_ack_version=3, pid=2502, random_key="d4e5f6"
    )

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""

    # expect output for unsynced run
    assert f"Synchronising {get_qualified_name(unsynced_container)}" in captured.out
    assert (
        f"Synchronization of {container_type.value} {get_qualified_name(unsynced_container)} completed." in captured.out
    )

    # expect NO output for synced run
    assert f"Synchronising {get_qualified_name(synced_container)}" not in captured.out

    # and
    assert backend.execute_operations.called_once()
    backend.execute_operations.assert_has_calls(
        calls=[
            mocker.call(
                container_id=unsynced_container.id,
                container_type=container_type,
                operations=["op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_all_offline_v2_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    offline_run = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None, pid=2501, random_key="a1b2c3"
    )

    # and
    get_run_impl = generate_get_metadata_container(registered_containers=(offline_run,))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_container",
        lambda project, container_type: offline_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Offline container run__{}__2501__a1b2c3 registered as {}".format(
            f"{offline_run.id}", get_qualified_name(offline_run)
        )
    ) in captured.out

    # and
    backend.execute_operations.assert_has_calls(
        [
            mocker.call(
                container_id=offline_run.id,
                container_type=ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_selected_v2_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    unsync_exp = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1, pid=2501, random_key="a1b2c3"
    )  # won't be synced, despite fact it's not synced yet
    sync_exp = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3, pid=2502, random_key="d4e5f6"
    )  # will be synced despite fact that it's up to date
    offline_run = prepare_v2_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None, pid=2503, random_key="g7h8j9"
    )  # will be synced

    # and
    get_run_impl = generate_get_metadata_container(
        registered_containers=[
            unsync_exp,
            sync_exp,
            offline_run,
        ]
    )

    # and
    mocker.patch.object(backend, "get_metadata_container", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_container",
        lambda project, container_type: offline_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_selected_containers(
        base_path=tmp_path,
        project_name="some-name",
        container_names=[
            get_qualified_name(sync_exp),
            "offline/run__" + offline_run.id + "__2503__g7h8j9",
        ],
    )

    # then
    captured = capsys.readouterr()
    # expect no errors
    assert captured.err == ""

    # expected output for mentioned async exp
    assert "Synchronising {}".format(get_qualified_name(sync_exp)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(sync_exp)) in captured.out

    # expected output for offline container
    assert (
        f"Offline container run__{offline_run.id}__2503__g7h8j9 registered as {get_qualified_name(offline_run)}"
    ) in captured.out
    assert "Synchronising {}".format(get_qualified_name(offline_run)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(offline_run)) in captured.out

    # expected NO output for not mentioned async container
    assert "Synchronising {}".format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    backend.execute_operations.assert_has_calls(
        [
            mocker.call(
                container_id=offline_run.id,
                container_type=ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_sync_all_v1_containers(tmp_path, mocker, capsys, backend, sync_runner, container_type):
    # given
    unsynced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=1)
    synced_container = prepare_v1_container(container_type=container_type, path=tmp_path, last_ack_version=3)

    # and
    get_container_impl = generate_get_metadata_container(registered_containers=(unsynced_container, synced_container))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""

    # expect output for unsynced run
    assert f"Synchronising {get_qualified_name(unsynced_container)}" in captured.out
    assert (
        f"Synchronization of {container_type.value} {get_qualified_name(unsynced_container)} completed." in captured.out
    )

    # expect NO output for synced run
    assert f"Synchronising {get_qualified_name(synced_container)}" not in captured.out

    # and
    assert backend.execute_operations.called_once()
    backend.execute_operations.assert_has_calls(
        calls=[
            mocker.call(
                container_id=unsynced_container.id,
                container_type=container_type,
                operations=["op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_all_offline_v1_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    offline_run = prepare_v1_container(container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None)

    # and
    get_run_impl = generate_get_metadata_container(registered_containers=(offline_run,))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_container",
        lambda project, container_type: offline_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Offline container run__{} registered as {}".format(f"{offline_run.id}", get_qualified_name(offline_run))
    ) in captured.out

    # and
    backend.execute_operations.assert_has_calls(
        [
            mocker.call(
                container_id=offline_run.id,
                container_type=ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_selected_v1_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    unsync_exp = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1
    )  # won't be synced, despite fact it's not synced yet
    sync_exp = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3
    )  # will be synced despite fact that it's up to date
    offline_run = prepare_v1_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None
    )  # will be synced

    # and
    get_run_impl = generate_get_metadata_container(
        registered_containers=[
            unsync_exp,
            sync_exp,
            offline_run,
        ]
    )

    # and
    mocker.patch.object(backend, "get_metadata_container", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_container",
        lambda project, container_type: offline_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_selected_containers(
        base_path=tmp_path,
        project_name="some-name",
        container_names=[
            get_qualified_name(sync_exp),
            "offline/run__" + offline_run.id,
        ],
    )

    # then
    captured = capsys.readouterr()
    # expect no errors
    assert captured.err == ""

    # expected output for mentioned async exp
    assert "Synchronising {}".format(get_qualified_name(sync_exp)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(sync_exp)) in captured.out

    # expected output for offline container
    assert (
        "Offline container run__{} registered as {}".format(f"{offline_run.id}", get_qualified_name(offline_run))
    ) in captured.out
    assert "Synchronising {}".format(get_qualified_name(offline_run)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(offline_run)) in captured.out

    # expected NO output for not mentioned async container
    assert "Synchronising {}".format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    backend.execute_operations.assert_has_calls(
        [
            mocker.call(
                container_id=offline_run.id,
                container_type=ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_v0_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    deprecated_unsynced_run = prepare_v0_run(path=tmp_path, last_ack_version=1)
    offline_old_run = prepare_v0_run(path=tmp_path, last_ack_version=None)

    # and
    get_container_impl = generate_get_metadata_container(
        registered_containers=(deprecated_unsynced_run, offline_old_run)
    )

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_container",
        lambda project, container_type: offline_old_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""

    assert (
        "Offline container {} registered as {}".format(f"{offline_old_run.id}", get_qualified_name(offline_old_run))
    ) in captured.out

    assert "Synchronising {}".format(get_qualified_name(deprecated_unsynced_run)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(deprecated_unsynced_run)) in captured.out
    assert "Synchronising {}".format(get_qualified_name(offline_old_run)) in captured.out
    assert "Synchronization of run {} completed.".format(get_qualified_name(offline_old_run)) in captured.out

    # and
    backend.execute_operations.assert_has_calls(
        [
            mocker.call(
                container_id=deprecated_unsynced_run.id,
                container_type=ContainerType.RUN,
                operations=["op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
            mocker.call(
                container_id=offline_old_run.id,
                container_type=ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
                operation_storage=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_sync_non_existent_container(tmp_path, capsys, sync_runner):
    # when
    sync_runner.sync_selected_containers(base_path=tmp_path, project_name="foo", container_names=["bar"])

    # then
    captured = capsys.readouterr()
    assert "Warning: Run 'bar' does not exist in location" in captured.out


def test_sync_non_existent_offline_containers(tmp_path, capsys, sync_runner):
    # when
    sync_runner.sync_selected_containers(base_path=tmp_path, project_name="foo", container_names=["offline/foo__bar"])
    sync_runner.sync_selected_containers(base_path=tmp_path, project_name="foo", container_names=["offline/model__bar"])

    # then
    captured = capsys.readouterr()
    assert "Offline container foo__bar not found on disk." in captured.out
    assert "Offline container model__bar not found on disk." in captured.out
