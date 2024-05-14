import random
import threading
import unittest
from unittest.mock import (
    Mock,
    patch,
)

from neptune.core.operation_processors.async_operation_processor.processing_resources import ProcessingResources
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import UniqueId


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
class TestProcessingResourcesCleanup(unittest.TestCase):
    def test_cleanup_of_resources_called(self):
        # given
        processing_resources = ProcessingResources(
            container_id=UniqueId("test_id"),
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
            container_id=UniqueId("test_id"),
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
