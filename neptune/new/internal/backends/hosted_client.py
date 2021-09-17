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
import platform
from typing import Dict, Tuple

from bravado.client import SwaggerClient
from bravado.http_client import HttpClient
from bravado.requests_client import RequestsClient
from packaging import version

from neptune.new.internal.backends.utils import (
    with_api_exceptions_handler,
    build_operation_url,
    update_session_proxies,
    verify_host_resolution,
    create_swagger_client,
    verify_client_version,
    cache,
)
from neptune.new.internal.backends.api_model import ClientConfig
from neptune.new.internal.credentials import Credentials
from neptune.new.version import version as neptune_client_version
from neptune.new.exceptions import UnsupportedClientVersion
# TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
from neptune.oauth import NeptuneAuthenticator

BACKEND_SWAGGER_PATH = "/api/backend/swagger.json"
LEADERBOARD_SWAGGER_PATH = "/api/leaderboard/swagger.json"
ARTIFACTS_SWAGGER_PATH = "/api/artifacts/swagger.json"

CONNECT_TIMEOUT = 30  # helps detecting internet connection lost
REQUEST_TIMEOUT = None

DEFAULT_REQUEST_KWARGS = {
    '_request_options': {
        "connect_timeout": CONNECT_TIMEOUT,
        "timeout": REQUEST_TIMEOUT,
        "headers": {"X-Neptune-LegacyClient": "false"}
    }
}


def create_http_client(ssl_verify: bool, proxies: Dict[str, str]) -> RequestsClient:
    http_client = RequestsClient(ssl_verify=ssl_verify)
    http_client.session.verify = ssl_verify

    update_session_proxies(http_client.session, proxies)

    user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
        lib_version=neptune_client_version,
        system=platform.platform(),
        python_version=platform.python_version())
    http_client.session.headers.update({'User-Agent': user_agent})

    return http_client


@cache
def _get_token_client(
        credentials: Credentials,
        ssl_verify: bool,
        proxies: Dict[str, str],
        endpoint_url: str = None
) -> SwaggerClient:
    config_api_url = credentials.api_url_opt or credentials.token_origin_address
    if proxies is None:
        verify_host_resolution(config_api_url)

    token_http_client = create_http_client(ssl_verify, proxies)

    return create_swagger_client(
        build_operation_url(endpoint_url or config_api_url, BACKEND_SWAGGER_PATH),
        token_http_client
    )


@cache
@with_api_exceptions_handler
def get_client_config(credentials: Credentials, ssl_verify: bool, proxies: Dict[str, str]) -> ClientConfig:
    backend_client = _get_token_client(credentials=credentials, ssl_verify=ssl_verify, proxies=proxies)

    config = backend_client.api.getClientConfig(
        X_Neptune_Api_Token=credentials.api_token,
        alpha="true",
        **DEFAULT_REQUEST_KWARGS,
    ).response().result

    if hasattr(config, "pyLibVersions"):
        min_recommended = getattr(config.pyLibVersions, "minRecommendedVersion", None)
        min_compatible = getattr(config.pyLibVersions, "minCompatibleVersion", None)
        max_compatible = getattr(config.pyLibVersions, "maxCompatibleVersion", None)
    else:
        raise UnsupportedClientVersion(neptune_client_version, max_version="0.4.111")

    return ClientConfig(
        api_url=config.apiUrl,
        display_url=config.applicationUrl,
        min_recommended_version=version.parse(min_recommended) if min_recommended else None,
        min_compatible_version=version.parse(min_compatible) if min_compatible else None,
        max_compatible_version=version.parse(max_compatible) if max_compatible else None
    )


@cache
def create_http_client_with_auth(
        credentials: Credentials,
        ssl_verify: bool,
        proxies: Dict[str, str]
) -> Tuple[RequestsClient, ClientConfig]:
    client_config = get_client_config(
        credentials=credentials,
        ssl_verify=ssl_verify,
        proxies=proxies
    )

    config_api_url = credentials.api_url_opt or credentials.token_origin_address

    verify_client_version(client_config, neptune_client_version)

    endpoint_url = None
    if config_api_url != client_config.api_url:
        endpoint_url = build_operation_url(client_config.api_url, BACKEND_SWAGGER_PATH)

    http_client = create_http_client(ssl_verify=ssl_verify, proxies=proxies)
    http_client.authenticator = NeptuneAuthenticator(
        credentials.api_token,
        _get_token_client(
            credentials=credentials,
            ssl_verify=ssl_verify,
            proxies=proxies,
            endpoint_url=endpoint_url
        ),
        ssl_verify,
        proxies
    )

    return http_client, client_config


@cache
def create_backend_client(client_config: ClientConfig, http_client: HttpClient) -> SwaggerClient:
    return create_swagger_client(
        build_operation_url(client_config.api_url, BACKEND_SWAGGER_PATH),
        http_client
    )


@cache
def create_leaderboard_client(client_config: ClientConfig, http_client: HttpClient) -> SwaggerClient:
    return create_swagger_client(
        build_operation_url(client_config.api_url, LEADERBOARD_SWAGGER_PATH),
        http_client
    )


@cache
def create_artifacts_client(client_config: ClientConfig, http_client: HttpClient) -> SwaggerClient:
    return create_swagger_client(
        build_operation_url(client_config.api_url, ARTIFACTS_SWAGGER_PATH),
        http_client
    )
