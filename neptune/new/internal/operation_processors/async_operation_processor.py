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
import logging
import os
import sys
import uuid
from threading import Event
from time import time, sleep
from typing import Optional, List

import click

from neptune.new.exceptions import NeptuneConnectionLostException
from neptune.new.internal.containers.storage_queue import StorageQueue
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.threading.daemon import Daemon

# pylint: disable=protected-access

_logger = logging.getLogger(__name__)


class AsyncOperationProcessor(OperationProcessor):

    def __init__(self,
                 run_uuid: uuid.UUID,
                 queue: StorageQueue[Operation],
                 backend: NeptuneBackend,
                 sleep_time: float = 5,
                 batch_size: int = 1000):
        self._run_uuid = run_uuid
        self._queue = queue
        self._backend = backend
        self._batch_size = batch_size
        self._last_version = 0
        self._consumed_version = 0
        self._waiting_for_version = 0
        self._waiting_event = Event()
        self._consumer = self.ConsumerThread(self, sleep_time, batch_size)
        self._drop_operations = False

        if sys.version_info >= (3, 7):
            try:
                # pylint: disable=no-member
                os.register_at_fork(after_in_child=self._handle_fork_in_child)
            except AttributeError:
                pass

    def _handle_fork_in_child(self):
        self._drop_operations = True

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        if self._drop_operations:
            return
        self._last_version = self._queue.put(op)
        if self._queue.size() > self._batch_size / 2:
            self._consumer.wake_up()
        if wait:
            self.wait()

    def wait(self):
        self.flush()
        self._waiting_for_version = self._last_version
        if self._consumed_version >= self._waiting_for_version:
            self._waiting_for_version = 0
            return
        self._consumer.wake_up()
        self._waiting_event.wait()
        self._waiting_event.clear()

    def flush(self):
        self._queue.flush()

    def start(self):
        self._consumer.start()

    def stop(self, seconds: Optional[float] = None):
        ts = time()
        self._queue.flush()
        if self._consumer.is_running():
            self._consumer.disable_sleep()
            self._consumer.wake_up()
            self._queue.wait_for_empty(seconds)
            self._consumer.interrupt()
        sec_left = None if seconds is None else seconds - (time() - ts)
        self._consumer.join(sec_left)
        # Do not close queue. According to specification only synchronization thread should be stopped.
        # self._queue.close()

    class ConsumerThread(Daemon):

        RETRIES = 10
        RETRY_WAIT = 30

        def __init__(self,
                     processor: 'AsyncOperationProcessor',
                     sleep_time: float,
                     batch_size: int):
            super().__init__(sleep_time=sleep_time)
            self._processor = processor
            self._batch_size = batch_size
            self._last_flush = 0

        def work(self) -> None:
            ts = time()
            if ts - self._last_flush >= self._sleep_time:
                self._last_flush = ts
                self._processor._queue.flush()

            while True:
                batch, version = self._processor._queue.get_batch(self._batch_size)
                if not batch:
                    return
                self.process_batch(batch, version)

        def process_batch(self, batch: List[Operation], version: int) -> None:
            # TODO: Handle Metadata errors
            for retry in range(0, self.RETRIES):
                try:
                    result = self._processor._backend.execute_operations(self._processor._run_uuid, batch)
                    self._processor._queue.ack(version)
                    for error in result:
                        _logger.error("Error occurred during asynchronous operation processing: %s", error)
                    break
                except NeptuneConnectionLostException:
                    if retry >= self.RETRIES - 1:
                        click.echo("Experiencing connection interruptions. Killing Neptune asynchronous thread. "
                                   "All data is safe on disk.",
                                   sys.stderr)
                        raise
                    click.echo("Experiencing connection interruptions. Reestablishing communication with Neptune.",
                               sys.stderr)
                    sleep(self.RETRY_WAIT)
                except Exception:
                    click.echo("Unexpected error occurred. Killing Neptune asynchronous thread. "
                               "All data is safe on disk.",
                               sys.stderr)
                    raise

            self._processor._consumed_version = version
            if self._processor._waiting_for_version > 0:
                if self._processor._consumed_version >= self._processor._waiting_for_version:
                    self._processor._waiting_for_version = 0
                    self._processor._waiting_event.set()
