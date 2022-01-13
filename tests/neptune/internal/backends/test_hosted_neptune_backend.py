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

import mock
from mock import MagicMock

from neptune.exceptions import (
    DeprecatedApiToken,
    CannotResolveHostname,
    UnsupportedClientVersion,
)
from neptune.internal.api_clients import HostedNeptuneBackendApiClient

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUuYWkiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)


@mock.patch(
    "neptune.internal.api_clients.hosted_api_clients.hosted_backend_api_client.NeptuneAuthenticator",
    new=MagicMock,
)
class TestHostedNeptuneBackend(unittest.TestCase):
    # pylint:disable=protected-access

    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch("neptune.__version__", "0.5.13")
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.13")

        # expect
        HostedNeptuneBackendApiClient(api_token=API_TOKEN)

    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch("neptune.__version__", "0.5.13")
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.14")

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackendApiClient(api_token=API_TOKEN)

        self.assertTrue("Please install neptune-client>=0.5.14" in str(ex.exception))

    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch("neptune.__version__", "0.5.13")
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.5.13")

        # expect
        HostedNeptuneBackendApiClient(api_token=API_TOKEN)

    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch("neptune.__version__", "0.5.13")
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.5.12")

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackendApiClient(api_token=API_TOKEN)

        self.assertTrue("Please install neptune-client==0.5.12" in str(ex.exception))

    # pylint: disable=unused-argument
    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch(
        "neptune.internal.api_clients.credentials.os.getenv", return_value=API_TOKEN
    )
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_should_take_default_credentials_from_env(
        self, env, swagger_client_factory
    ):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        # when
        backend = HostedNeptuneBackendApiClient()

        # then
        self.assertEqual(API_TOKEN, backend.credentials.api_token)

    @mock.patch("bravado.client.SwaggerClient.from_url")
    @mock.patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_should_accept_given_api_token(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        # when
        session = HostedNeptuneBackendApiClient(API_TOKEN)

        # then
        self.assertEqual(API_TOKEN, session.credentials.api_token)

    @mock.patch("socket.gethostbyname")
    def test_deprecated_token(self, gethostname_mock):
        # given
        token = (
            "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkuc3RhZ2UubmVwdHVuZS5tbCIsImF"
            "waV9rZXkiOiI5ODM4ZDk1NC00MDAzLTExZTktYmY1MC0yMzE5ODM1NWRhNjYifQ=="
        )

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(DeprecatedApiToken):
            HostedNeptuneBackendApiClient(token)

    @mock.patch("socket.gethostbyname")
    def test_cannot_resolve_host(self, gethostname_mock):
        # given
        token = (
            "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkuc3RhZ2UubmVwdHVuZS5tbCIsImFwaV91cmwiOiJodHRwczovL3VpLn"
            "N0YWdlLm5lcHR1bmUuYWkiLCJhcGlfa2V5IjoiOTgzOGQ5NTQtNDAwMy0xMWU5LWJmNTAtMjMxOTgzNTVkYTY2In0="
        )

        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackendApiClient(token)

    @staticmethod
    def _get_swagger_client_mock(
        swagger_client_factory,
        min_recommended=None,
        min_compatible=None,
        max_compatible=None,
    ):
        py_lib_versions = type("py_lib_versions", (object,), {})()
        setattr(py_lib_versions, "minRecommendedVersion", min_recommended)
        setattr(py_lib_versions, "minCompatibleVersion", min_compatible)
        setattr(py_lib_versions, "maxCompatibleVersion", max_compatible)

        artifacts = type("artifacts", (object,), {})()
        setattr(artifacts, "enabled", True)

        multipart_upload = type("multiPartUpload", (object,), {})()
        setattr(multipart_upload, "enabled", True)
        setattr(multipart_upload, "minChunkSize", 5242880)
        setattr(multipart_upload, "maxChunkSize", 1073741824)
        setattr(multipart_upload, "maxChunkCount", 1000)
        setattr(multipart_upload, "maxSinglePartSize", 5242880)

        client_config = type("client_config_response_result", (object,), {})()
        setattr(client_config, "pyLibVersions", py_lib_versions)
        setattr(client_config, "artifacts", artifacts)
        setattr(client_config, "multiPartUpload", multipart_upload)
        setattr(client_config, "apiUrl", None)
        setattr(client_config, "applicationUrl", None)

        swagger_client = MagicMock()
        swagger_client.api.getClientConfig.return_value.response.return_value.result = (
            client_config
        )
        swagger_client_factory.return_value = swagger_client

        return swagger_client


class SomeClass(object):
    pass


if __name__ == "__main__":
    unittest.main()
