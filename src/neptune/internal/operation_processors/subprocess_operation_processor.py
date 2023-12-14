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
__all__ = ("SubprocessOperationProcessor",)

from collections import namedtuple
from enum import Enum
from multiprocessing import (
    Process,
    Queue,
)
from queue import Queue as ThreadingQueue
from threading import RLock
from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune.internal.backends.factory import get_backend
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.threading.daemon import Daemon
from neptune.types.mode import Mode

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId
    from neptune.internal.operation import Operation
    from neptune.internal.signals_processing.signals import Signal


class RequestType(str, Enum):
    OPERATION = "operation"
    FLUSH = "flush"
    PAUSE = "pause"
    START = "start"
    RESUME = "resume"
    WAIT = "wait"
    STOP = "stop"
    CLOSE = "close"


class ResponseType(str, Enum):
    ACK = "ack"
    EXCEPTION = "exception"


RequestMessage = namedtuple("RequestMessage", ["type", "payload", "ack"])
ResponseMessage = namedtuple("ResponseMessage", ["type", "payload"])
SignalMessage = namedtuple("SignalMessage", ["type", "payload"])


class CustomSignalsQueue:
    def __init__(self, queue: "Queue[SignalMessage]"):
        self._queue: "Queue[SignalMessage]" = queue

    def put_nowait(self, item: "Signal") -> None:
        self._queue.put(SignalMessage(type=type(item).__name__, payload=item))


class Worker(Process):
    def __init__(
        self,
        container_id: "UniqueId",
        container_type: "ContainerType",
        requests_queue: "Queue[RequestMessage]",
        responses_queue: "Queue[ResponseMessage]",
        signals_queue: "Queue[SignalMessage]",
        sleep_time: float = 5,
        batch_size: int = 1000,
        api_token: Optional[str] = None,
        proxies: Optional[dict] = None,
    ) -> None:
        super().__init__()
        self._requests_queue = requests_queue
        self._responses_queue = responses_queue
        self._container_id = container_id
        self._container_type = container_type
        self._sleep_time = sleep_time
        self._batch_size = batch_size
        self._api_token = api_token
        self._proxies = proxies

        self._signals_queue = CustomSignalsQueue(queue=signals_queue)
        self._lock: Optional[RLock] = None
        self._backend: Optional["NeptuneBackend"] = None
        self._op_processor: Optional["OperationProcessor"] = None

    def run(self) -> None:
        self._lock = RLock()
        self._backend = get_backend(mode=Mode.ASYNC, api_token=self._api_token, proxies=self._proxies)
        self._op_processor = get_operation_processor(
            mode=Mode.ASYNC,
            container_id=self._container_id,
            container_type=self._container_type,
            backend=self._backend,
            lock=self._lock,
            flush_period=self._sleep_time,
            queue=self._signals_queue,
        )

        while True:
            message = self._requests_queue.get()
            message_type, message_payload, should_ack = message.type, message.payload, message.ack

            try:
                if message_type == RequestType.PAUSE:
                    self._op_processor.pause()
                if message_type == RequestType.START:
                    self._op_processor.start()
                elif message_type == RequestType.RESUME:
                    self._op_processor.resume()
                elif message_type == RequestType.STOP:
                    seconds = message_payload.get("seconds", None)
                    self._op_processor.stop(seconds=seconds)
                    self._responses_queue.put(ResponseMessage(type=ResponseType.ACK, payload=None))
                    break
                elif message_type == RequestType.FLUSH:
                    self._op_processor.flush()
                elif message_type == RequestType.WAIT:
                    self._op_processor.wait()
                elif message_type == RequestType.CLOSE:
                    self._op_processor.close()
                elif message_type == RequestType.OPERATION:
                    self._op_processor.enqueue_operation(message_payload, wait=should_ack)

                self._responses_queue.put(ResponseMessage(type=ResponseType.ACK, payload=None))
            except KeyboardInterrupt:
                self._op_processor.stop()
                break
            except Exception as e:
                self._responses_queue.put(ResponseMessage(type=ResponseType.EXCEPTION, payload=e))
                raise


class SubprocessOperationProcessor(OperationProcessor):
    def __init__(
        self,
        container_id: "UniqueId",
        container_type: "ContainerType",
        signals_queue: "Queue[Signal]",
        sleep_time: float = 5,
        batch_size: int = 1000,
        api_token: Optional[str] = None,
        proxies: Optional[dict] = None,
    ) -> None:
        self._requests_queue: "Queue[RequestMessage]" = Queue()
        self._responses_queue: "Queue[ResponseMessage]" = Queue()
        self._worker_signals_queue: "Queue[SignalMessage]" = Queue()
        self._signals_queue: "ThreadingQueue[Signal]" = signals_queue
        self._signals_proxy = self.SignalsProxyThread(
            sleep_time=sleep_time,
            signals_queue=self._signals_queue,
            worker_signals_queue=self._worker_signals_queue,
        )
        self._worker: Process = Worker(
            container_id=container_id,
            container_type=container_type,
            requests_queue=self._requests_queue,
            responses_queue=self._responses_queue,
            signals_queue=self._worker_signals_queue,
            sleep_time=sleep_time,
            batch_size=batch_size,
            api_token=api_token,
            proxies=proxies,
        )
        self._started = False
        self.start()

    class SignalsProxyThread(Daemon):
        def __init__(
            self,
            sleep_time: float,
            signals_queue: "ThreadingQueue[Signal]",
            worker_signals_queue: "Queue[SignalMessage]",
        ):
            super().__init__(name="SignalsProxyThread", sleep_time=sleep_time)
            self._signals_queue = signals_queue
            self._worker_signals_queue = worker_signals_queue

        def work(self) -> None:
            while True:
                message = self._worker_signals_queue.get()
                _, message_payload = message.type, message.payload
                self._signals_queue.put_nowait(message_payload)

    def _wait_for_ack(self) -> None:
        response = self._responses_queue.get()
        if response.type == ResponseType.EXCEPTION:
            raise response.payload

    def enqueue_operation(self, op: "Operation", *, wait: bool) -> None:
        self._requests_queue.put(RequestMessage(type=RequestType.OPERATION, payload=op, ack=wait))
        self._wait_for_ack()

    def start(self) -> None:
        if self._started:
            return

        self._signals_proxy.start()
        self._worker.start()
        self._requests_queue.put(RequestMessage(type=RequestType.START, payload=None, ack=True))
        self._wait_for_ack()
        self._started = True

    def pause(self) -> None:
        self._signals_proxy.pause()
        self._requests_queue.put(RequestMessage(type=RequestType.PAUSE, payload=None, ack=True))
        self._wait_for_ack()

    def resume(self) -> None:
        self._signals_proxy.resume()
        self._requests_queue.put(RequestMessage(type=RequestType.RESUME, payload=None, ack=True))
        self._wait_for_ack()

    def flush(self) -> None:
        self._requests_queue.put(RequestMessage(type=RequestType.FLUSH, payload=None, ack=True))
        self._wait_for_ack()

    def wait(self) -> None:
        self._signals_proxy.wake_up()
        self._requests_queue.put(RequestMessage(type=RequestType.WAIT, payload=None, ack=True))
        self._wait_for_ack()

    def stop(self, seconds: Optional[float] = None) -> None:
        self._requests_queue.put(RequestMessage(type=RequestType.STOP, payload={"seconds": seconds}, ack=True))
        self._wait_for_ack()
        self._worker.join()
        if self._signals_proxy.is_running():
            self._signals_proxy.disable_sleep()
            self._signals_proxy.wake_up()
            self._signals_proxy.interrupt()

    def close(self) -> None:
        self._requests_queue.put(RequestMessage(type=RequestType.CLOSE, payload=None, ack=True))
        self._wait_for_ack()
        self._worker.join()
