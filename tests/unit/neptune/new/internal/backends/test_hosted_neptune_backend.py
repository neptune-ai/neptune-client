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
import socket
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import (
    Mock,
    call,
)

import pytest
from bravado.exception import (
    HTTPPaymentRequired,
    HTTPTooManyRequests,
    HTTPUnprocessableEntity,
)
from mock import (
    MagicMock,
    patch,
)
from packaging.version import Version

from neptune.core.components.operation_storage import OperationStorage
from neptune.exceptions import (
    CannotResolveHostname,
    FileUploadError,
    MetadataInconsistency,
    NeptuneClientUpgradeRequiredError,
    NeptuneLimitExceedException,
)
from neptune.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    _get_token_client,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
    get_client_config,
)
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.utils import verify_host_resolution
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.operation import (
    AssignString,
    LogFloats,
)
from tests.unit.neptune.backend_test_mixin import BackendTestMixin
from tests.unit.neptune.new.utils import response_mock

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUuYWkiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)

credentials = Credentials.from_token(API_TOKEN)


@patch("neptune.internal.backends.hosted_client.RequestsClient", new=MagicMock())
@patch("neptune.internal.backends.hosted_client.NeptuneAuthenticator", new=MagicMock())
@patch("bravado.client.SwaggerClient.from_url")
@patch("platform.platform", new=lambda: "testPlatform")
@patch("platform.python_version", new=lambda: "3.9.test")
class TestHostedNeptuneBackend(unittest.TestCase, BackendTestMixin):
    def setUp(self) -> None:
        # Clear all LRU storage
        verify_host_resolution.cache_clear()
        _get_token_client.cache_clear()
        get_client_config.cache_clear()
        create_http_client_with_auth.cache_clear()
        create_backend_client.cache_clear()
        create_leaderboard_client.cache_clear()

        self.container_types = [ContainerType.RUN, ContainerType.PROJECT]
        self.dummy_operation_storage = OperationStorage(Path("./tests/dummy_storage"))

    @patch("neptune.internal.backends.hosted_neptune_backend.create_backend_client")
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_execute_operations(self, upload_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations().response().result = [response_error]
        swagger_client.api.executeOperations.reset_mock()
        upload_mock.return_value = [FileUploadError("file1", "error2")]

        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                upload_mock.reset_mock()
                swagger_client_factory.reset_mock()

                # when
                result = backend.execute_operations(
                    container_id=container_uuid,
                    container_type=container_type,
                    operations=[
                        LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
                        AssignString(["properties", "name"], "some text"),
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                swagger_client.api.executeOperations.assert_called_once_with(
                    **{
                        "experimentId": str(container_uuid),
                        "operations": [
                            {
                                "path": "images/img1",
                                "logFloats": {
                                    "entries": [
                                        {
                                            "value": 1,
                                            "step": 2,
                                            "timestampMilliseconds": 3000,
                                        }
                                    ]
                                },
                            },
                            {
                                "path": "properties/name",
                                "assignString": {"value": "some text"},
                            },
                        ],
                        **DEFAULT_REQUEST_KWARGS,
                    }
                )

                self.assertEqual(
                    (
                        2,
                        [
                            MetadataInconsistency("error1"),
                        ],
                    ),
                    result,
                )

    @pytest.mark.asyncio
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    async def test_too_many_requests(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        container_type = ContainerType.RUN

        response = MagicMock()
        response.response().return_value = []

        retry_after_seconds = 5  # Przykładowy czas oczekiwania
        too_many_requests_response = HTTPTooManyRequests(MagicMock())
        too_many_requests_response.headers = {"retry-after": str(retry_after_seconds)}

        swagger_client.api.executeOperations.side_effect = Mock(
            side_effect=[too_many_requests_response, response_mock()]
        )

        # when
        result_start_time = time.time()
        result = await backend.execute_async(  # Użyj await, aby poczekać na wykonanie coroutine
            container_id=container_uuid,
            container_type=container_type,
            operations=[
                LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
            ],
            operation_storage=self.dummy_operation_storage,
        )
        result_end_time = time.time()

        # then
        self.assertEqual(result, (1, []))
        assert retry_after_seconds <= (result_end_time - result_start_time) <= (retry_after_seconds * 2)

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_execute_operations_retry_request(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        container_type = ContainerType.RUN

        response = MagicMock()
        response.response().return_value = []
        swagger_client.api.executeOperations.side_effect = Mock(
            side_effect=[HTTPTooManyRequests(MagicMock()), response_mock()]
        )

        # when
        result = backend.execute_operations(
            container_id=container_uuid,
            container_type=container_type,
            operations=[
                LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
            ],
            operation_storage=self.dummy_operation_storage,
        )

        # then
        self.assertEqual(result, (1, []))
        execution_operation_call = call(
            **{
                "experimentId": str(container_uuid),
                "operations": [
                    {
                        "path": "images/img1",
                        "logFloats": {
                            "entries": [
                                {
                                    "value": 1,
                                    "step": 2,
                                    "timestampMilliseconds": 3000,
                                }
                            ]
                        },
                    }
                ],
                **DEFAULT_REQUEST_KWARGS,
            }
        )
        swagger_client.api.executeOperations.assert_has_calls([execution_operation_call, execution_operation_call])

    @patch(
        "neptune.internal.backends.hosted_client.neptune_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.13")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.internal.backends.hosted_client.neptune_version",
        Version("2.0.0-alpha4+dev1234"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_pre_release_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="2.0.0")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.internal.backends.hosted_client.neptune_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.14")

        # expect
        with self.assertRaises(NeptuneClientUpgradeRequiredError) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("minimum required version is >=0.5.14" in str(ex.exception))

    @patch(
        "neptune.internal.backends.hosted_client.neptune_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.5.12")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.internal.backends.hosted_client.neptune_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.4.999")

        # expect
        with self.assertRaises(NeptuneClientUpgradeRequiredError) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("minimum required version is ==0.4.0" in str(ex.exception))

    @patch("socket.gethostbyname")
    def test_cannot_resolve_host(self, gethostname_mock, _):
        # given
        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackend(credentials)

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_limit_exceed(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        # when:
        error = response_mock()
        error.json.return_value = {"title": "Maximum storage limit reached"}
        swagger_client.api.executeOperations.side_effect = HTTPPaymentRequired(response=error)

        # then:
        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                with self.assertRaises(NeptuneLimitExceedException):
                    backend.execute_operations(
                        container_id=container_uuid,
                        container_type=container_type,
                        operations=[
                            LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                        ],
                        operation_storage=self.dummy_operation_storage,
                    )

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_limit_exceed_legacy(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        # when:
        error = response_mock()
        error.json.return_value = {"title": "Monitoring hours not left"}
        swagger_client.api.executeOperations.side_effect = HTTPUnprocessableEntity(response=error)

        # then:
        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                with self.assertRaises(NeptuneLimitExceedException):
                    backend.execute_operations(
                        container_id=container_uuid,
                        container_type=container_type,
                        operations=[
                            LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                        ],
                        operation_storage=self.dummy_operation_storage,
                    )
