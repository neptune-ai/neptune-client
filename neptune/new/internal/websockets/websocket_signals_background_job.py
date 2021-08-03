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
import json
import logging
import threading
from json.decoder import JSONDecodeError

from typing import TYPE_CHECKING, Optional

import click
from websocket import WebSocketConnectionClosedException

from neptune.internal.websockets.reconnecting_websocket import ReconnectingWebsocket
from neptune.new.attributes.constants import SIGNAL_TYPE_ABORT, SIGNAL_TYPE_STOP, SYSTEM_FAILED_ATTRIBUTE_PATH

from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.threading.daemon import Daemon
from neptune.new.internal.utils import process_killer
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
        if not self._started or threading.get_ident() == self._thread.ident:
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
                if raw_message is None or self._is_heartbeat(raw_message):
                    return
                else:
                    self._handler_message(raw_message)
            except WebSocketConnectionClosedException:
                pass

        def _handler_message(self, msg: str):
            try:
                json_msg = json.loads(msg)
                msg_type = json_msg.get("type")
                msg_body = json_msg.get("body")
                if not msg_type:
                    click.echo("Malformed websocket signal: missing type", err=True)
                    return
                if not isinstance(msg_type, str):
                    click.echo(f"Malformed websocket signal: type is {type(msg_type)}, should be str", err=True)
                    return
                if msg_type.lower() == SIGNAL_TYPE_STOP:
                    self._handle_stop(msg_body)
                elif msg_type.lower() == SIGNAL_TYPE_ABORT:
                    self._handle_abort(msg_body)
            except JSONDecodeError as ex:
                click.echo(f"Malformed websocket signal: {ex}, message: {msg}", err=True)

        def _handle_stop(self, msg_body):
            msg_body = msg_body or dict()
            if not isinstance(msg_body, dict):
                click.echo(f"Malformed websocket signal: body is {type(msg_body)}, should be dict", err=True)
                return
            run_id = self._run["sys/id"].fetch()
            click.echo(f"Run {run_id} received stop signal. Exiting", err=True)
            seconds = msg_body.get("seconds")
            self._run.stop(seconds=seconds)
            process_killer.kill_me()

        def _handle_abort(self, msg_body):
            msg_body = msg_body or dict()
            if not isinstance(msg_body, dict):
                click.echo(f"Malformed websocket signal: body is {type(msg_body)}, should be dict", err=True)
                return
            run_id = self._run["sys/id"].fetch()
            click.echo(f"Run {run_id} received abort signal. Exiting", err=True)
            seconds = msg_body.get("seconds")
            self._run[SYSTEM_FAILED_ATTRIBUTE_PATH] = True
            self._run.stop(seconds=seconds)
            process_killer.kill_me()

        def shutdown_ws_client(self):
            self._ws_client.shutdown()

        @staticmethod
        def _is_heartbeat(message: str):
            return message.strip() == ''
