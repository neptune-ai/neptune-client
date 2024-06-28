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
import unittest
from unittest.mock import (
    Mock,
    patch,
)

from neptune.core.operation_processors.async_operation_processor.processing_resources import ProcessingResources
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import CustomId


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
class TestProcessingResourcesCleanup(unittest.TestCase):
    def test_cleanup_of_resources_called(self):
        # given
        processing_resources = ProcessingResources(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processing_resources._data_path = Mock(wraps=processing_resources._data_path)
        processing_resources.metadata_file = Mock()
        processing_resources.disk_queue = Mock()
        processing_resources.operation_storage = Mock()

        # when
        processing_resources.cleanup()

        # then
        processing_resources.metadata_file.cleanup.assert_called_once()
        processing_resources.disk_queue.cleanup.assert_called_once()
        processing_resources.operation_storage.cleanup.assert_called_once()

    def test_cleanup_oserror_happens(self):
        # given
        processing_resources = ProcessingResources(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processing_resources._data_path = Mock(wraps=processing_resources._data_path)
        processing_resources._data_path.rmdir = Mock(side_effect=OSError)

        # when
        processing_resources.cleanup()

        # then
        processing_resources._data_path.rmdir.assert_called_once()
