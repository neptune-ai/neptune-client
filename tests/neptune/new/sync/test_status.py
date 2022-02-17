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

from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import Operation
from neptune.new.sync import StatusRunner
from neptune.new.sync.utils import get_qualified_name
from tests.neptune.new.sync.utils import (
    prepare_metadata_container,
    generate_get_metadata_container,
)


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


@pytest.fixture(name="status_runner")
def status_runner_fixture(backend):
    return StatusRunner(backend=backend)


@pytest.mark.parametrize("container_type", list(ContainerType))
def test_list_containers(
    tmp_path, mocker, capsys, backend, status_runner, container_type
):
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
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Synchronized objects:\n- {}".format(get_qualified_name(synced_container))
        in captured.out
    )
    assert (
        "Unsynchronized objects:\n- {}".format(get_qualified_name(unsynced_container))
        in captured.out
    )


def test_list_offline_runs(tmp_path, mocker, capsys, status_runner):
    # given
    offline_run = prepare_metadata_container(
        container_type=ContainerType.RUN,
        path=tmp_path,
        last_ack_version=None,
    )

    # and
    mocker.patch.object(Operation, "from_dict")

    # when
    status_runner.synchronization_status(tmp_path)

    # then
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (
        "Unsynchronized offline objects:\n- offline/run__{}".format(offline_run.id)
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
    assert "There are no Neptune objects" in captured.out
