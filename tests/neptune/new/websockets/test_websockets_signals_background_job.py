#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

# pylint: disable=protected-access

import unittest

from mock import patch, MagicMock

from neptune.new.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob


class TestClient(unittest.TestCase):

    @patch('neptune.new.internal.websockets.websocket_signals_background_job.process_killer')
    def test_listener_stop(self, process_killer):
        # given
        run, ws = MagicMock(), MagicMock()
        listener = WebsocketSignalsBackgroundJob._ListenerThread(run, ws)

        ws.recv.return_value = '{"type": "neptune/stop", "body": {"seconds": 5}}'

        # when
        listener.work()

        # then
        self.assertEqual(0, run.__setitem__.call_count)
        run.stop.assert_called_once_with(seconds=5)
        process_killer.kill_me.assert_called_once_with()

    @patch('neptune.new.internal.websockets.websocket_signals_background_job.process_killer')
    def test_listener_abort(self, process_killer):
        # given
        run, ws = MagicMock(), MagicMock()
        listener = WebsocketSignalsBackgroundJob._ListenerThread(run, ws)

        ws.recv.return_value = '{"type": "neptune/abort", "body": {"seconds": 5}}'

        # when
        listener.work()

        # then
        run.__setitem__.assert_called_once_with("sys/failed", True)
        run.stop.assert_called_once_with(seconds=5)
        process_killer.kill_me.assert_called_once_with()
