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

import time
import unittest
from typing import List

import mock
from freezegun import freeze_time
from mock import MagicMock

from neptune.internal.api_clients import HostedNeptuneBackendApiClient
from neptune.internal.api_clients.hosted_api_clients.hosted_alpha_leaderboard_api_client import \
    HostedAlphaLeaderboardApiClient
from neptune.internal.channels.channels import ChannelIdWithValues, ChannelNamespace, ChannelValue, ChannelType
from tests.neptune.new.backend_test_mixin import BackendTestMixin as AlphaBackendTestMixin

API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYWxwaGEuc3RhZ2UubmVwdHVuZS5haSIsImFwaV91cmwiOiJodHRwczovL2FscG' \
            'hhLnN0YWdlLm5lcHR1bmUuYWkiLCJhcGlfa2V5IjoiZDg5MGQ3Y2ItZGEzNi00MjRkLWJhNTQtZmVjZDJmYTdhOTQzIn0='
"""base64 decoded `API_TOKEN`
{
  "api_address": "https://alpha.stage.neptune.ai",
  "api_url": "https://alpha.stage.neptune.ai",
  "api_key": "d890d7cb-da36-424d-ba54-fecd2fa7a943"
}
"""


class TestAlphaIntegrationNeptuneBackend(unittest.TestCase, AlphaBackendTestMixin):
    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.internal.api_clients.hosted_api_clients.hosted_backend_api_client.NeptuneAuthenticator',
                new=MagicMock)
    @mock.patch('neptune.new.internal.backends.hosted_neptune_backend.NeptuneAuthenticator', new=MagicMock)
    def setUp(self, swagger_client_factory) -> None:
        # pylint:disable=arguments-differ
        self._get_swagger_client_mock(swagger_client_factory)
        self.backend = HostedNeptuneBackendApiClient(API_TOKEN)
        self.leaderboard = HostedAlphaLeaderboardApiClient(self.backend)
        self.exp_mock = MagicMock(
            internal_id='00000000-0000-0000-0000-000000000000'
        )

    @freeze_time()
    def _test_send_channel_values(
            self, channel_y_elements: List[tuple], expected_operation: str, channel_type: ChannelType):
        # given prepared `ChannelIdWithValues`
        channel_id = 'channel_id'
        channel_name = 'channel_name'
        now_ms = int(time.time() * 1000)
        channel_with_values = ChannelIdWithValues(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_type=channel_type.value,
            channel_namespace=ChannelNamespace.USER,
            channel_values=[
                ChannelValue(x=None, y={channel_y_key: channel_y_value}, ts=None)
                for channel_y_key, channel_y_value in channel_y_elements
            ]
        )

        # invoke send_channels_values
        self.leaderboard.send_channels_values(self.exp_mock, [channel_with_values])

        # expect `executeOperations` was called once with properly prepared kwargs
        expected_call_args = {
            'experimentId': '00000000-0000-0000-0000-000000000000',
            'operations': [{
                'path': f'logs/{channel_name}',
                expected_operation: {
                    'entries': [
                        {'value': channel_y_value, 'step': None, 'timestampMilliseconds': now_ms}
                        for _, channel_y_value in channel_y_elements
                    ]
                }
            }]
        }
        # pylint:disable=protected-access
        execute_operations = self.leaderboard.leaderboard_swagger_client.api.executeOperations
        self.assertEqual(len(execute_operations.call_args_list), 1)
        self.assertDictEqual(execute_operations.call_args_list[0][1], expected_call_args)

    def test_send_channels_text_values(self):
        channel_y_elements = [
            ('text_value', 'Line of text'),
            ('text_value', 'Another line of text'),
        ]
        self._test_send_channel_values(
            channel_y_elements, expected_operation='logStrings', channel_type=ChannelType.TEXT)

    def test_send_channels_numeric_values(self):
        channel_y_elements = [
            ('numeric_value', 42),
            ('numeric_value', 0.07),
        ]
        self._test_send_channel_values(
            channel_y_elements, expected_operation='logFloats', channel_type=ChannelType.NUMERIC)

    def test_send_channels_image_values(self):
        """TODO: implement in NPT-9207"""
