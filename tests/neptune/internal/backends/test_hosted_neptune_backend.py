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
import base64
import socket
import unittest
import uuid

import mock
from mock import MagicMock

from neptune.exceptions import DeprecatedApiToken, CannotResolveHostname
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from tests.neptune.api_models import ApiParameter


API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ' \
            'hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0='


@mock.patch('neptune.internal.backends.hosted_neptune_backend.NeptuneAuthenticator', new=MagicMock)
class TestHostedNeptuneBackend(unittest.TestCase):
    # pylint:disable=protected-access

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('uuid.uuid4')
    def test_convert_to_api_parameters(self, uuid4, swagger_client_factory):
        # given
        swagger_client = MagicMock()
        swagger_client.get_model.return_value = ApiParameter
        swagger_client_factory.return_value = swagger_client

        # and
        some_uuid = str(uuid.uuid4())
        uuid4.return_value = some_uuid

        # and
        backend = HostedNeptuneBackend(api_token=API_TOKEN)

        # and
        some_object = SomeClass()

        # when
        api_params = backend._convert_to_api_parameters({
            'str': 'text',
            'bool': False,
            'float': 1.23,
            'int': int(12),
            'list': [123, 'abc', ['def']],
            'obj': some_object
        })

        # then
        expected_api_params = {
            ApiParameter(id=some_uuid, name='str', parameterType='string', value='text'),
            ApiParameter(id=some_uuid, name='bool', parameterType='string', value='False'),
            ApiParameter(id=some_uuid, name='float', parameterType='double', value='1.23'),
            ApiParameter(id=some_uuid, name='int', parameterType='double', value='12'),
            ApiParameter(id=some_uuid, name='list', parameterType='string', value="[123, 'abc', ['def']]"),
            ApiParameter(id=some_uuid, name='obj', parameterType='string', value=str(some_object))
        }
        self.assertEqual(expected_api_params, set(api_params))

    # pylint: disable=unused-argument
    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.internal.backends.credentials.os.getenv', return_value=API_TOKEN)
    def test_should_take_default_credentials_from_env(self, env, swagger_client_factory):
        # when
        backend = HostedNeptuneBackend()

        # then
        self.assertEqual(API_TOKEN, backend.credentials.api_token)

    @mock.patch('bravado.client.SwaggerClient.from_url')
    def test_should_accept_given_api_token(self, _):
        # when
        session = HostedNeptuneBackend(API_TOKEN)

        # then
        self.assertEqual(API_TOKEN, session.credentials.api_token)

    @mock.patch('socket.gethostbyname')
    def test_depracted_token_error_msg(self, gethostname_mock):
        # given
        token_json = '{"api_address":"https://ui.stage.neptune.ml","api_key":"9838d954-4003-11e9-bf50-23198355da66"}'
        token = str(base64.encodebytes(token_json.encode('utf-8')), 'utf-8')

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(DeprecatedApiToken):
            HostedNeptuneBackend(token)

    @mock.patch('socket.gethostbyname')
    def test_depracted_token_error_msg(self, gethostname_mock):
        # given
        token_json = '{"api_address":"https://ui.stage.neptune.ml",' \
                     '"api_url":"https://ui.stage.neptune.ai",' \
                     '"api_key":"9838d954-4003-11e9-bf50-23198355da66"}'
        token = str(base64.encodebytes(token_json.encode('utf-8')), 'utf-8')

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackend(token)

class SomeClass(object):
    pass


if __name__ == '__main__':
    unittest.main()
