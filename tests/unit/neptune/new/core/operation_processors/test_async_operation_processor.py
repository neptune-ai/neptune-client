#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import random
import threading
from pathlib import Path
from unittest.mock import (
    Mock,
    patch,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.core.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import UniqueId


@patch("neptune.core.operation_processors.async_operation_processor.MetadataFile", new=Mock)
class TestAsyncOperationProcessorInit:
    def test_resources(self):
        # given
        processor = AsyncOperationProcessor(
            container_id=UniqueId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
        )

        # then
        assert processor.resources == (processor._metadata_file, processor._operation_storage, processor._queue)

    @patch(
        "neptune.core.operation_processors.async_operation_processor.get_container_full_path",
        return_value=Path("mock_path"),
    )
    # those patches prevent the side effect of creating test directories
    @patch("neptune.core.components.operation_storage.os.makedirs", new=lambda *_, **__: None)
    @patch("neptune.core.operation_processors.async_operation_processor.DiskQueue", new=Mock)
    def test_data_path(self, mock_get_container_full_path):
        # given
        test_cases = [
            {"data_path": Path("test_data_path"), "expected": Path("test_data_path")},
            {"data_path": None, "expected": Path("mock_path")},
        ]

        for test_case in test_cases:
            container_type = random.choice(list(ContainerType))
            processor = AsyncOperationProcessor(
                container_id=UniqueId("test_id"),
                container_type=container_type,
                lock=threading.RLock(),
                data_path=test_case["data_path"],
            )
            # then
            assert processor.data_path == test_case["expected"]

            if test_case["data_path"] is None:
                mock_get_container_full_path.assert_called_once_with(
                    ASYNC_DIRECTORY,
                    UniqueId("test_id"),
                    container_type,
                )
