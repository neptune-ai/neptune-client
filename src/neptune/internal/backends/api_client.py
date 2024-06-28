#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

__all__ = ["TokenRefreshingURLs", "get_config_and_token_urls", "create_auth_api_client"]


from dataclasses import dataclass
from typing import Tuple

from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.models import (
    ClientConfig,
    Error,
)


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> "TokenRefreshingURLs":
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def get_config_and_token_urls(credentials: Credentials) -> Tuple[ClientConfig, TokenRefreshingURLs]:
    with Client(base_url=credentials.base_url) as client:
        config = get_client_config.sync(client=client)
        if config is None or isinstance(config, Error):
            raise RuntimeError(f"Failed to get client config: {config}")
        response = client.get_httpx_client().get(config.security.open_id_discovery)
        token_urls = TokenRefreshingURLs.from_dict(response.json())
    return config, token_urls


def create_auth_api_client(
    credentials: Credentials, config: ClientConfig, token_refreshing_urls: TokenRefreshingURLs
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
    )
