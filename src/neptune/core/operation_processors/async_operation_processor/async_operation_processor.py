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

__all__ = ("AsyncOperationProcessor",)

import threading
from pathlib import Path
from queue import Queue
from time import time
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
)

from neptune.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune.core.operation_processors.async_operation_processor.constants import (
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
)
from neptune.core.operation_processors.async_operation_processor.consumer_thread import ConsumerThread
from neptune.core.operation_processors.async_operation_processor.operation_logger import ProcessorStopSignal
from neptune.core.operation_processors.async_operation_processor.processing_resources import ProcessingResources
from neptune.core.operation_processors.async_operation_processor.queue_observer import QueueObserver
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.operations.operation import Operation
from neptune.core.operations.utils import try_get_step
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import CustomId
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.signals_processing.signals import Signal
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)


class AsyncOperationProcessor(WithResources, OperationProcessor):
    def __init__(
        self,
        custom_id: CustomId,
        container_type: ContainerType,
        lock: threading.RLock,
        signal_queue: "Queue[Signal]",
        batch_size: int = 1,
        data_path: Optional[Path] = None,
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict(),
        should_print_logs: bool = True,
        sleep_time: float = 5.0,
    ) -> None:
        self._should_print_logs = should_print_logs
        self._accepts_operations: bool = True
        self._last_version: int = 0

        self._processing_resources = ProcessingResources(
            batch_size=batch_size,
            custom_id=custom_id,
            container_type=container_type,
            lock=lock,
            signal_queue=signal_queue,
            data_path=data_path,
            serializer=serializer,
        )

        self._consumer = ConsumerThread(
            sleep_time=sleep_time,
            processing_resources=self._processing_resources,
        )

        self._queue_observer = QueueObserver(
            disk_queue=self._processing_resources.disk_queue,
            consumer=self._consumer,
            should_print_logs=self._should_print_logs,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self._processing_resources.resources

    @property
    def data_path(self) -> Path:
        return self._processing_resources.data_path

    @property
    def processing_resources(self) -> "ProcessingResources":
        return self._processing_resources

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        if not self._accepts_operations:
            warn_once("Not accepting operations", exception=NeptuneWarning)
            return

        step = try_get_step(op)
        self._last_version = self.processing_resources.disk_queue.put(op, category=step)

        if _queue_has_enough_space(self.processing_resources.disk_queue.size(), self._processing_resources.batch_size):
            self._consumer.wake_up()
        if wait:
            self.wait()

    def start(self) -> None:
        self._consumer.start()

    def pause(self) -> None:
        self._consumer.pause()
        self.flush()

    def resume(self) -> None:
        self._consumer.resume()

    def wait(self) -> None:
        self.flush()
        waiting_for_version = self._last_version
        self._consumer.wake_up()

        # Probably reentering lock just for sure
        with self._processing_resources.waiting_cond:
            self._processing_resources.waiting_cond.wait_for(
                lambda: self._processing_resources.consumed_version >= waiting_for_version
                or not self._consumer.is_running()
            )
        if not self._consumer.is_running():
            raise NeptuneSynchronizationAlreadyStoppedException()

    def stop(
        self,
        seconds: Optional[float] = None,
        processor_stop_signal_queue: Optional["Queue[ProcessorStopSignal]"] = None,
    ) -> None:
        ts = time()
        self.flush()
        if self._consumer.is_running():
            self._consumer.disable_sleep()
            self._consumer.wake_up()
            self._queue_observer.wait_for_queue_empty(
                seconds=seconds,
                processor_stop_signal_queue=processor_stop_signal_queue,
            )
            self._consumer.interrupt()
        sec_left = None if seconds is None else seconds - (time() - ts)
        self._consumer.join(sec_left)

        # Close resources
        self.close()

        # Remove local files
        if self._queue_observer.is_queue_empty():
            self.cleanup()

    def cleanup(self) -> None:
        self._processing_resources.cleanup()

    def close(self) -> None:
        self._accepts_operations = False
        super().close()


def _queue_has_enough_space(queue_size: int, batch_size: int) -> bool:
    return queue_size > batch_size / 2
