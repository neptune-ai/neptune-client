#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from uuid import uuid4

from mock import (
    MagicMock,
    patch,
)

from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor


@patch("neptune.internal.operation_processors.async_operation_processor.DiskQueue")
@patch("neptune.internal.operation_processors.async_operation_processor.OperationStorage")
@patch("neptune.internal.operation_processors.async_operation_processor.MetadataFile")
def test_close(metadata_file_mock, _, disk_queue_mock):
    # given
    container_id = UniqueId(str(uuid4()))
    container_type = ContainerType.RUN

    # and
    metadata_file = metadata_file_mock.return_value
    disk_queue = disk_queue_mock.return_value

    # and
    processor = AsyncOperationProcessor(
        container_id=container_id,
        container_type=container_type,
        backend=MagicMock(),
        lock=MagicMock(),
        queue=MagicMock(),
    )

    # and
    processor.start()

    # when
    processor.close()

    # then
    disk_queue.close.assert_called_once()
    metadata_file.close.assert_called_once()


@patch("neptune.internal.operation_processors.async_operation_processor.DiskQueue")
@patch("neptune.internal.operation_processors.async_operation_processor.OperationStorage")
@patch("neptune.internal.operation_processors.async_operation_processor.MetadataFile")
def test_cleanup_if_empty(metadata_file_mock, operation_storage_mock, disk_queue_mock):
    # given
    container_id = UniqueId(str(uuid4()))
    container_type = ContainerType.RUN

    # and
    metadata_file = metadata_file_mock.return_value
    operation_storage = operation_storage_mock.return_value
    disk_queue = disk_queue_mock.return_value
    disk_queue.is_empty.return_value = True
    disk_queue.size.return_value = 0

    # and
    processor = AsyncOperationProcessor(
        container_id=container_id,
        container_type=container_type,
        backend=MagicMock(),
        lock=MagicMock(),
        queue=MagicMock(),
    )

    # and
    processor.start()

    # when
    processor.stop(seconds=1)

    # then
    disk_queue.close.assert_called()
    metadata_file.close.assert_called()
    disk_queue.is_empty.assert_called()

    disk_queue.cleanup_if_empty.assert_called()
    operation_storage.cleanup.assert_not_called()
    metadata_file.cleanup.assert_called()


@patch("neptune.internal.operation_processors.async_operation_processor.DiskQueue")
@patch("neptune.internal.operation_processors.async_operation_processor.OperationStorage")
@patch("neptune.internal.operation_processors.async_operation_processor.MetadataFile")
def test_metadata(metadata_file_mock, _, __):
    # given
    container_id = UniqueId(str(uuid4()))
    container_type = ContainerType.RUN

    # when
    AsyncOperationProcessor(
        container_id=container_id,
        container_type=container_type,
        backend=MagicMock(),
        lock=MagicMock(),
        queue=MagicMock(),
    )

    # then
    metadata = metadata_file_mock.call_args_list[0][1]["metadata"]
    assert metadata["mode"] == "async"
    assert metadata["containerType"] == ContainerType.RUN
    assert metadata["containerId"] == container_id
