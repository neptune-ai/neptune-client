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
    generate_get_metadata_container,
    prepare_metadata_container,
    execute_operations,
    prepare_deprecated_run,
)


@pytest.fixture(name="backend")
def backend_fixture():
    backend = MagicMock()
    backend.execute_operations.side_effect = execute_operations
    return backend


@pytest.fixture(name="sync_runner")
def sync_runner_fixture(backend):
    return SyncRunner(backend=backend)


@pytest.mark.parametrize("container_type", list(ContainerType))
def test_sync_all_runs(tmp_path, mocker, capsys, backend, sync_runner, container_type):
    # given
    unsynced_container = prepare_metadata_container(
        container_type=container_type, path=tmp_path, last_ack_version=1
    )
    synced_container = prepare_metadata_container(
        container_type=container_type, path=tmp_path, last_ack_version=3
    )
    get_container_impl = generate_get_metadata_container(
        registered_containers=(unsynced_container, synced_container)
    )

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
        f"Synchronization of {container_type.value} {get_qualified_name(unsynced_container)} completed."
        in captured.out
    )

    # expect NO output for synced run
    assert f"Synchronising {get_qualified_name(synced_container)}" not in captured.out

    # and
    backend.execute_operations.has_calls(
        [
            mocker.call(unsynced_container.id, ContainerType.RUN, ["op-1", "op-2"]),
        ],
        any_order=True,
    )


def test_sync_all_offline_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    offline_run = prepare_metadata_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None
    )
    get_run_impl = generate_get_metadata_container(registered_containers=(offline_run,))

    # and
    mocker.patch.object(backend, "get_metadata_container", get_run_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_run",
        lambda project, container_type: offline_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Offline run run__{} registered as {}".format(
            f"{offline_run.id}", get_qualified_name(offline_run)
        )
    ) in captured.out

    # and
    backend.execute_operations.has_calls(
        [
            mocker.call(offline_run.id, ContainerType.RUN, ["op-1", "op-2"]),
        ],
        any_order=True,
    )


def test_sync_selected_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    unsync_exp = prepare_metadata_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=1
    )  # won't be synced, despite fact it's not synced yet
    sync_exp = prepare_metadata_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=3
    )  # will be synced despite fact that it's up to date
    offline_run = prepare_metadata_container(
        container_type=ContainerType.RUN, path=tmp_path, last_ack_version=None
    )  # will be synced
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
        "_register_offline_run",
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
    assert (
        "Synchronization of run {} completed.".format(get_qualified_name(sync_exp))
        in captured.out
    )

    # expected output for offline container
    assert (
        "Offline run run__{} registered as {}".format(
            f"{offline_run.id}", get_qualified_name(offline_run)
        )
    ) in captured.out
    assert "Synchronising {}".format(get_qualified_name(offline_run)) in captured.out
    assert (
        "Synchronization of run {} completed.".format(get_qualified_name(offline_run))
        in captured.out
    )

    # expected NO output for not mentioned async container
    assert "Synchronising {}".format(get_qualified_name(unsync_exp)) not in captured.out

    # and
    backend.execute_operations.has_calls(
        [
            mocker.call(
                sync_exp.id,
                ContainerType.RUN,
                operations=["op-1", "op-2"],
            ),
            mocker.call(
                offline_run.id,
                ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
            ),
        ],
        any_order=True,
    )


def test_sync_deprecated_runs(tmp_path, mocker, capsys, backend, sync_runner):
    # given
    deprecated_unsynced_run = prepare_deprecated_run(path=tmp_path, last_ack_version=1)
    offline_old_run = prepare_deprecated_run(path=tmp_path, last_ack_version=None)
    get_container_impl = generate_get_metadata_container(
        registered_containers=(deprecated_unsynced_run, offline_old_run)
    )

    # and
    mocker.patch.object(backend, "get_metadata_container", get_container_impl)
    mocker.patch.object(
        sync_runner,
        "_register_offline_run",
        lambda project, container_type: offline_old_run,
    )
    mocker.patch.object(Operation, "from_dict", lambda x: x)

    # when
    sync_runner.sync_all_containers(tmp_path, "foo")

    # then
    captured = capsys.readouterr()
    assert captured.err == ""

    assert (
        "Offline run {} registered as {}".format(
            f"{offline_old_run.id}", get_qualified_name(offline_old_run)
        )
    ) in captured.out

    assert (
        "Synchronising {}".format(get_qualified_name(deprecated_unsynced_run))
        in captured.out
    )
    assert (
        "Synchronization of run {} completed.".format(
            get_qualified_name(deprecated_unsynced_run)
        )
        in captured.out
    )
    assert (
        "Synchronising {}".format(get_qualified_name(offline_old_run)) in captured.out
    )
    assert (
        "Synchronization of run {} completed.".format(
            get_qualified_name(offline_old_run)
        )
        in captured.out
    )

    # and
    backend.execute_operations.has_calls(
        [
            mocker.call(
                deprecated_unsynced_run.id,
                ContainerType.RUN,
                operations=["op-1", "op-2"],
            ),
            mocker.call(
                offline_old_run.id,
                ContainerType.RUN,
                operations=["op-0", "op-1", "op-2"],
            ),
        ],
        any_order=True,
    )


def test_sync_non_existent_container(tmp_path, capsys, sync_runner):
    # when
    sync_runner.sync_selected_containers(
        base_path=tmp_path, project_name="foo", container_names=["bar"]
    )

    # then
    captured = capsys.readouterr()
    assert "Warning: Run 'bar' does not exist in location" in captured.err


def test_sync_non_existent_offline_containers(tmp_path, capsys, sync_runner):
    # when
    sync_runner.sync_selected_containers(
        base_path=tmp_path, project_name="foo", container_names=["offline/foo__bar"]
    )
    sync_runner.sync_selected_containers(
        base_path=tmp_path, project_name="foo", container_names=["offline/model__bar"]
    )

    # then
    captured = capsys.readouterr()
    assert "Offline run foo__bar not found on disk." in captured.err
    assert "Offline run model__bar not found on disk." in captured.err
