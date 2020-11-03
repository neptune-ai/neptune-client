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
import re

from neptune.internal.websockets.reconnecting_websocket import ReconnectingWebsocket


class ReconnectingWebsocketFactory(object):
    def __init__(self, backend, experiment_id):
        self._backend = backend
        self._base_address = re.sub(r'^http', 'ws', self._backend.api_address) + '/api/notifications/v1'
        self._experiment_id = experiment_id

    def create(self, shutdown_condition):
        url = self._experiment_url(self._base_address, self._experiment_id)
        return ReconnectingWebsocket(
            url=url,
            oauth2_session=self._backend.authenticator.auth.session,
            shutdown_event=shutdown_condition,
            proxies=self._backend.proxies)

    @staticmethod
    def _experiment_url(base_address, experiment_id):
        return base_address + '/experiments/' + experiment_id + '/operations'
