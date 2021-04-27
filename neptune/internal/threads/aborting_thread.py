#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import threading

from websocket import WebSocketConnectionClosedException

from neptune.internal.threads.neptune_thread import NeptuneThread
from neptune.internal.websockets.message import MessageType
from neptune.internal.websockets.websocket_message_processor import WebsocketMessageProcessor


class AbortingThread(NeptuneThread):
    def __init__(self, websocket_factory, abort_impl, experiment):
        super(AbortingThread, self).__init__(is_daemon=True)
        self._abort_message_processor = AbortMessageProcessor(abort_impl, experiment)
        self._ws_client = websocket_factory.create(shutdown_condition=threading.Event())

    def run(self):
        try:
            while self.should_continue_running():
                raw_message = self._ws_client.recv()
                self._abort_message_processor.run(raw_message)
        except WebSocketConnectionClosedException:
            pass

    def shutdown(self):
        self.interrupt()
        self._ws_client.shutdown()

    @staticmethod
    def _is_heartbeat(message):
        return message.strip() == ''


class AbortMessageProcessor(WebsocketMessageProcessor):
    def __init__(self, abort_impl, experiment):
        super(AbortMessageProcessor, self).__init__()
        self._abort_impl = abort_impl
        self._experiment = experiment
        self.received_abort_message = False

    def _process_message(self, message):
        if message.get_type() == MessageType.STOP:
            self._experiment.stop()
            self._abort()
        elif message.get_type() == MessageType.ABORT:
            self._experiment.stop("Remotely aborted")
            self._abort()

    def _abort(self):
        self.received_abort_message = True
        self._abort_impl.abort()
