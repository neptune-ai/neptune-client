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

__all__ = ["ConsumerThread"]

from time import time
from typing import (
    List,
    Optional,
)

from neptune.core.operation_processors.async_operation_processor.processing_resources import ProcessingResources
from neptune.core.operations.operation import Operation
from neptune.internal.daemon import Daemon
from neptune.internal.signals_processing.utils import (
    signal_batch_lag,
    signal_batch_processed,
    signal_batch_started,
)


class ConsumerThread(Daemon):
    def __init__(
        self,
        sleep_time: float,
        processing_resources: ProcessingResources,
    ) -> None:
        super().__init__(sleep_time=sleep_time, name="NeptuneAsyncOpProcessor")
        self._processing_resources = processing_resources
        self._last_flush: float = 0.0

    def run(self) -> None:
        try:
            super().run()
        except Exception as e:
            with self._processing_resources.waiting_cond:
                self._processing_resources.waiting_cond.notify_all()
            raise Exception from e

    def work(self) -> None:
        ts = time()
        if ts - self._last_flush >= self._sleep_time:
            self._last_flush = ts
            self._processing_resources.disk_queue.flush()

        while True:
            batch = self._processing_resources.disk_queue.get_batch(self._processing_resources.batch_size)
            if not batch:
                return

            signal_batch_started(queue=self._processing_resources.signals_queue)
            self.process_batch([element.obj for element in batch], batch[-1].ver, batch[-1].at)

    @Daemon.ConnectionRetryWrapper(
        kill_message=(
            "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
            " synced manually using `neptune sync` command."
        )
    )
    def process_batch(self, batch: List[Operation], version: int, occurred_at: Optional[float] = None) -> None:
        if occurred_at is not None:
            signal_batch_lag(queue=self._processing_resources.signals_queue, lag=time() - occurred_at)

        expected_count = len(batch)
        version_to_ack = version - expected_count
        while True:
            # TODO: Add proper batch processing logic here once backend is ready

            signal_batch_processed(queue=self._processing_resources.signals_queue)
            processed_count = len(batch)
            version_to_ack += processed_count

            with self._processing_resources.waiting_cond:
                self._processing_resources.disk_queue.ack(version_to_ack)

                self._processing_resources.consumed_version = version_to_ack

                if version_to_ack == version:
                    self._processing_resources.waiting_cond.notify_all()
                    return
