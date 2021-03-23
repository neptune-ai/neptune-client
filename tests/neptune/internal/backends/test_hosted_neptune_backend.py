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
import socket
import unittest
import uuid

import mock
from mock import call, MagicMock

from neptune.exceptions import DeprecatedApiToken, CannotResolveHostname, UnsupportedClientVersion
from neptune.internal.api_clients import HostedNeptuneBackendApiClient
from neptune.internal.api_clients.hosted_api_clients.hosted_leaderboard_api_client import \
    HostedNeptuneLeaderboardApiClient
from tests.neptune.api_models import ApiParameter

API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ' \
            'hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0='


@mock.patch('neptune.internal.api_clients.hosted_api_clients.hosted_backend_api_client.NeptuneAuthenticator',
            new=MagicMock)
class TestHostedNeptuneBackend(unittest.TestCase):
    # pylint:disable=protected-access

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.__version__', '0.5.13')
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.13')

        # expect
        HostedNeptuneBackendApiClient(api_token=API_TOKEN)

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.__version__', '0.5.13')
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.14')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackendApiClient(api_token=API_TOKEN)

        self.assertTrue("Please install neptune-client>=0.5.14" in str(ex.exception))

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.__version__', '0.5.13')
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.5.13')

        # expect
        HostedNeptuneBackendApiClient(api_token=API_TOKEN)

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.__version__', '0.5.13')
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.5.12')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackendApiClient(api_token=API_TOKEN)

        self.assertTrue("Please install neptune-client==0.5.12" in str(ex.exception))

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('uuid.uuid4')
    def test_convert_to_api_parameters(self, uuid4, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        swagger_client.get_model.return_value = ApiParameter

        # and
        some_uuid = str(uuid.uuid4())
        uuid4.return_value = some_uuid

        # and
        backend = HostedNeptuneBackendApiClient(api_token=API_TOKEN)
        leaderboard = HostedNeptuneLeaderboardApiClient(backend)

        # and
        some_object = SomeClass()

        # when
        api_params = leaderboard._convert_to_api_parameters({
            'str': 'text',
            'bool': False,
            'float': 1.23,
            'int': int(12),
            'inf': float('inf'),
            '-inf': float('-inf'),
            'nan': float('nan'),
            'list': [123, 'abc', ['def']],
            'obj': some_object
        })

        # then
        expected_api_params = {
            ApiParameter(id=some_uuid, name='str', parameterType='string', value='text'),
            ApiParameter(id=some_uuid, name='bool', parameterType='string', value='False'),
            ApiParameter(id=some_uuid, name='float', parameterType='double', value='1.23'),
            ApiParameter(id=some_uuid, name='int', parameterType='double', value='12'),
            ApiParameter(id=some_uuid, name='inf', parameterType='string', value='Infinity'),
            ApiParameter(id=some_uuid, name='-inf', parameterType='string', value='-Infinity'),
            ApiParameter(id=some_uuid, name='nan', parameterType='string', value='NaN'),
            ApiParameter(id=some_uuid, name='list', parameterType='string', value="[123, 'abc', ['def']]"),
            ApiParameter(id=some_uuid, name='obj', parameterType='string', value=str(some_object))
        }
        self.assertEqual(expected_api_params, set(api_params))

    # pylint: disable=unused-argument
    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.internal.api_clients.credentials.os.getenv', return_value=API_TOKEN)
    def test_should_take_default_credentials_from_env(self, env, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        # when
        backend = HostedNeptuneBackendApiClient()

        # then
        self.assertEqual(API_TOKEN, backend.credentials.api_token)

    @mock.patch('bravado.client.SwaggerClient.from_url')
    def test_should_accept_given_api_token(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        # when
        session = HostedNeptuneBackendApiClient(API_TOKEN)

        # then
        self.assertEqual(API_TOKEN, session.credentials.api_token)

    @mock.patch('socket.gethostbyname')
    def test_depracted_token(self, gethostname_mock):
        # given
        token = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkuc3RhZ2UubmVwdHVuZS5tbCIsImF' \
                'waV9rZXkiOiI5ODM4ZDk1NC00MDAzLTExZTktYmY1MC0yMzE5ODM1NWRhNjYifQ=='

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(DeprecatedApiToken):
            HostedNeptuneBackendApiClient(token)

    @mock.patch('socket.gethostbyname')
    def test_cannot_resolve_host(self, gethostname_mock):
        # given
        token = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkuc3RhZ2UubmVwdHVuZS5tbCIsImFwaV91cmwiOiJodHRwczovL3VpLn' \
                'N0YWdlLm5lcHR1bmUuYWkiLCJhcGlfa2V5IjoiOTgzOGQ5NTQtNDAwMy0xMWU5LWJmNTAtMjMxOTgzNTVkYTY2In0='

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackendApiClient(token)

    @mock.patch('bravado.client.SwaggerClient.from_url')
    @mock.patch('neptune.__version__', '0.5.13')
    def test_delete_artifact(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        experiment = mock.MagicMock()
        backend = HostedNeptuneBackendApiClient(API_TOKEN)
        leaderboard = HostedNeptuneLeaderboardApiClient(backend)
        leaderboard.rm_data = mock.MagicMock()

        # and
        def build_call(path):
            return call(
                experiment=experiment,
                path=path
            )

        # when
        leaderboard.delete_artifacts(experiment=experiment, path='/an_abs_path_in_exp_output')
        leaderboard.delete_artifacts(experiment=experiment, path='/../an_abs_path_in_exp')
        leaderboard.delete_artifacts(experiment=experiment, path='/../../an_abs_path_in_prj')
        leaderboard.delete_artifacts(experiment=experiment, path='a_path_in_exp_output')
        self.assertRaises(ValueError, leaderboard.delete_artifacts,
                          experiment=experiment, path='test/../../a_path_outside_exp')
        self.assertRaises(ValueError, leaderboard.delete_artifacts,
                          experiment=experiment, path='../a_path_outside_exp')
        self.assertRaises(ValueError, leaderboard.delete_artifacts,
                          experiment=experiment, path="..")

        # then
        leaderboard.rm_data.assert_has_calls([
            build_call('/an_abs_path_in_exp_output'),
            build_call('/../an_abs_path_in_exp'),
            build_call('/../../an_abs_path_in_prj'),
            build_call('a_path_in_exp_output'),
        ])

    @staticmethod
    def _get_swagger_client_mock(
            swagger_client_factory,
            min_recommended=None,
            min_compatible=None,
            max_compatible=None):
        py_lib_versions = type('py_lib_versions', (object,), {})()
        setattr(py_lib_versions, "minRecommendedVersion", min_recommended)
        setattr(py_lib_versions, "minCompatibleVersion", min_compatible)
        setattr(py_lib_versions, "maxCompatibleVersion", max_compatible)

        client_config = type('client_config_response_result', (object,), {})()
        setattr(client_config, "pyLibVersions", py_lib_versions)
        setattr(client_config, "apiUrl", None)
        setattr(client_config, "applicationUrl", None)

        swagger_client = MagicMock()
        swagger_client.api.getClientConfig.return_value.response.return_value.result = client_config
        swagger_client_factory.return_value = swagger_client

        return swagger_client


class SomeClass(object):
    pass


if __name__ == '__main__':
    unittest.main()
