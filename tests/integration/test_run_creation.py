#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
import datetime
from neptune import (
    Run,
    ANONYMOUS_API_TOKEN,
)
from neptune.internal.backends.factory import get_backend

import responses
from responses import _recorder
from typing import Dict
from base64 import b64encode
from json import dumps, load
from neptune.types.mode import Mode
from neptune.internal.container_type import ContainerType
import jwt
from neptune.internal.backends.api_model import StringAttribute


def token() -> Dict[str, str]:
    return {
        "api_address": "https://testing.neptune.ai",
        "api_url": "https://testing.neptune.ai",
        "api_key": "abcdef-0123456789",
    }


def encoded_token() -> str:
    return b64encode(dumps(token()).encode('utf-8')).decode('utf-8')


# @_recorder.record(file_path="out.yaml")
# def test_recorder():
#     with Run():
#         ...


@responses.activate
def test_run_creation():
    with open('swaggers/artifacts-swagger.json') as handler:
        artifacts_swagger_response = responses.Response(
            method="GET",
            url="https://testing.neptune.ai/api/artifacts/swagger.json",
            json=load(handler),
            status=200,
        )
        responses.add(artifacts_swagger_response)

    with open('swaggers/leaderboard-swagger.json') as handler:
        leaderboard_swagger_response = responses.Response(
            method="GET",
            url="https://testing.neptune.ai/api/leaderboard/swagger.json",
            json=load(handler),
            status=200,
        )
        responses.add(leaderboard_swagger_response)

    with open('swaggers/backend-swagger.json') as handler:
        backend_swagger_response = responses.Response(
            method="GET",
            url="https://testing.neptune.ai/api/backend/swagger.json",
            json=load(handler),
            status=200,
        )
        responses.add(backend_swagger_response)

    config_response = responses.Response(
        method="GET",
        url="https://testing.neptune.ai/api/backend/v1/clients/config?alpha=true",
        json={
            "apiUrl": "https://testing.neptune.ai",
            "applicationUrl": "https://testing.neptune.ai",
            "pyLibVersions": {
                "minRecommendedVersion": "0.14.2",
                "minCompatibleVersion": "0.14.2"
            },
            "multiPartUpload": {
                "enabled": "true",
                "minChunkSize": 5242880,
                "maxChunkSize": 1073741824,
                "maxChunkCount": 1000,
                "maxSinglePartSize": 5242880
            },
            "artifacts": {
                "enabled": "true"
            }
        },
        status=200,
    )
    responses.add(config_response)

    oauth_response = responses.Response(
        method="GET",
        url="https://testing.neptune.ai/api/backend/v1/authorization/oauth-token",
        json={
            "accessToken": jwt.encode(
                payload={
                    "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1),
                    "iss": "https://testing.neptune.ai/auth/realms/neptune"
                },
                key="abc",
                algorithm="HS256"
            ),
            "refreshToken": jwt.encode(
                payload={
                    "iss": "https://testing.neptune.ai/auth/realms/neptune",
                    "aud": "https://testing.neptune.ai/auth/realms/neptune"
                },
                key="abc",
                algorithm="HS256"
            ),
            "username": "tester",
        },
        status=200,
    )
    responses.add(oauth_response)

    token_refresh = responses.Response(
        method="POST",
        url="https://testing.neptune.ai/auth/realms/neptune/protocol/openid-connect/token",
        body=dumps({
            "access_token": jwt.encode(
                payload={
                    "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1),
                    "iss": "https://testing.neptune.ai/auth/realms/neptune"
                },
                key="abc",
                algorithm="HS256"
            ),
            "refresh_token": jwt.encode(
                payload={
                    "iss": "https://testing.neptune.ai/auth/realms/neptune",
                    "aud": "https://testing.neptune.ai/auth/realms/neptune"
                },
                key="abc",
                algorithm="HS256"
            ),
            "token_type": "Bearer",
            "not-before-policy": 1501576317,
            "session_state": "29414a8c-2945-404b-bc1e-c0c7a27f0a61",
            "scope": "offline_access"
        }),
        status=200,
    )
    responses.add(token_refresh)

    abc = responses.Response(
        method="GET",
        url="https://app.neptune.ai/api/leaderboard/v1/attributes/strings?experimentId=99ec34df-8045-4833-8c86-74801bbbb4a5&attribute=sys%2Fid",
        json={
            "attributeName": "sys/id",
            "attributeType": "string",
            "value": "TES-120"
        },
        status=200,
    )
    responses.add(abc)

    backend = get_backend(mode=Mode.ASYNC, api_token=encoded_token())
    assert backend.get_string_attribute(
        container_id="99ec34df-8045-4833-8c86-74801bbbb4a5",
        container_type=ContainerType.RUN,
        path=["sys", "id"]
    ) == StringAttribute("TES-120")
    backend.close()
