import logging
import threading
from typing import Optional

from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.operation_processors.operation_processor import OperationProcessor

_logger = logging.getLogger(__name__)


class PartitionedOperationProcessor(OperationProcessor):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        backend: NeptuneBackend,
        lock: threading.RLock,
        sleep_time: float = 5,
        batch_size: int = 1000,
        partitions: int = 5,
    ):
        self.container_id = container_id
        self.partitions = partitions
        self.processors = []
        for i in range(partitions):
            self.processors.append(
                AsyncOperationProcessor(container_id, container_type, backend, lock, sleep_time, batch_size)
            )

    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        op_hash = hash(frozenset(op.path))
        partition_ = self.processors[op_hash % self.partitions]
        partition_.enqueue_operation(op, wait=wait)

    def wait(self) -> None:
        for processor in self.processors:
            processor.wait()

    def flush(self) -> None:
        for processor in self.processors:
            processor.flush()

    def start(self) -> None:
        for processor in self.processors:
            processor.start()

    def stop(self, seconds: Optional[float] = None) -> None:
        for processor in self.processors:
            processor.stop(seconds=seconds)

    def close(self) -> None:
        for processor in self.processors:
            processor.close()
