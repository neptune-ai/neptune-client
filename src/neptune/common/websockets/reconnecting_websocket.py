#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from websocket import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
)

from neptune.common.websockets.websocket_client_adapter import (
    WebsocketClientAdapter,
    WebsocketNotConnectedException,
)


class ReconnectingWebsocket(object):
    def __init__(self, url, oauth2_session, shutdown_event, proxies=None):
        self.url = url
        self.client = WebsocketClientAdapter()
        self._shutdown_event = shutdown_event
        self._oauth2_session = oauth2_session
        self._reconnect_counter = ReconnectCounter()
        self._token = oauth2_session.token
        self._proxies = proxies

    def shutdown(self):
        self._shutdown_event.set()
        self.client.close()
        self.client.abort()
        self.client.shutdown()

    def recv(self):
        if not self.client.connected:
            self._try_to_establish_connection()
        while self._is_active():
            try:
                data = self.client.recv()
                self._on_successful_connect()
                return data
            except WebSocketTimeoutException:
                raise
            except WebSocketConnectionClosedException:
                if self._is_active():
                    self._handle_lost_connection()
                else:
                    raise
            except WebsocketNotConnectedException:
                if self._is_active():
                    self._handle_lost_connection()
            except Exception:
                if self._is_active():
                    self._handle_lost_connection()

    def _is_active(self):
        return not self._shutdown_event.is_set()

    def _on_successful_connect(self):
        self._reconnect_counter.clear()

    def _try_to_establish_connection(self):
        try:
            self._request_token_refresh()
            if self.client.connected:
                self.client.shutdown()
            self.client.connect(url=self.url, token=self._token, proxies=self._proxies)
        except Exception:
            self._shutdown_event.wait(self._reconnect_counter.calculate_delay())

    def _handle_lost_connection(self):
        self._reconnect_counter.increment()
        self._try_to_establish_connection()

    def _request_token_refresh(self):
        self._token = self._oauth2_session.refresh_token(token_url=self._oauth2_session.auto_refresh_url)


class ReconnectCounter(object):
    MAX_RETRY_DELAY = 128

    def __init__(self):
        self.retries = 0

    def clear(self):
        self.retries = 0

    def increment(self):
        self.retries += 1

    def calculate_delay(self):
        return self._compute_delay(self.retries, self.MAX_RETRY_DELAY)

    @classmethod
    def _compute_delay(cls, attempt, max_delay):
        delay = cls._full_jitter_delay(attempt, max_delay)
        return delay

    @classmethod
    def _full_jitter_delay(cls, attempt, cap):
        exp = min(2 ** (attempt - 1), cap)
        return random.uniform(0, exp)
