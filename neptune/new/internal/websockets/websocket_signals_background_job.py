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

from typing import TYPE_CHECKING, Optional

from websocket import WebSocketConnectionClosedException

from neptune.internal.websockets.reconnecting_websocket import ReconnectingWebsocket

from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.threading.daemon import Daemon
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory

if TYPE_CHECKING:
    from neptune.new.run import Run

_logger = logging.getLogger(__name__)


class WebsocketSignalsBackgroundJob(BackgroundJob):

    def __init__(self, ws_factory: WebsocketsFactory):
        self._ws_factory = ws_factory
        self._thread: 'Optional[WebsocketSignalsBackgroundJob._ListenerThread]' = None
        self._started = False

    def start(self, run: 'Run'):
        self._thread = self._ListenerThread(run, self._ws_factory.create())
        self._thread.start()
        self._started = True

    def stop(self):
        if not self._started:
            return
        self._thread.interrupt()
        self._thread.shutdown_ws_client()

    def join(self, seconds: Optional[float] = None):
        if not self._started:
            return
        self._thread.join(seconds)

    class _ListenerThread(Daemon):

        def __init__(self, run: 'Run', ws_client: ReconnectingWebsocket):
            super().__init__(sleep_time=0)
            self._run = run
            self._ws_client = ws_client

        def work(self) -> None:
            try:
                raw_message = self._ws_client.recv()
                if self._is_heartbeat(raw_message):
                    return
                # Handle messages here
            except WebSocketConnectionClosedException:
                pass

        def shutdown_ws_client(self):
            self._ws_client.shutdown()

        @staticmethod
        def _is_heartbeat(message: str):
            return message.strip() == ''
