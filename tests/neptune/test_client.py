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
import unittest
import uuid

import mock
from mock import MagicMock

from neptune.client import Client
from tests.neptune.api_models import ApiParameter


@mock.patch('neptune.client.NeptuneAuthenticator', new=MagicMock)
class TestClient(unittest.TestCase):
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
        client = Client(api_address='some address', api_token='some token')

        # and
        some_object = SomeClass()

        # when
        api_params = client._convert_to_api_parameters({
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


class SomeClass(object):
    pass


if __name__ == '__main__':
    unittest.main()
