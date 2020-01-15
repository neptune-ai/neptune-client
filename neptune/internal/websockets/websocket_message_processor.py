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
import json

from neptune.internal.websockets.message import Message


class WebsocketMessageProcessor(object):
    def __init__(self):
        pass

    def run(self, raw_message):
        # Atmosphere framework sends heartbeat messages every minute, we have to ignore them
        if raw_message is not None and not self._is_heartbeat(raw_message):
            message = Message.from_json(json.loads(raw_message))
            self._process_message(message)

    def _process_message(self, message):
        raise NotImplementedError()

    @staticmethod
    def _is_heartbeat(message):
        return message.strip() == ''
