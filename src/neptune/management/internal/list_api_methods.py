__all__ = [
    "list_api_methods_used_by_client",
]

import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from neptune.common.envs import API_TOKEN_ENV_NAME
from neptune.internal.backends.hosted_client import (
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
)
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.internal.backends.utils import ssl_verify
from neptune.internal.credentials import Credentials


def _get_token(api_token: Optional[str] = None) -> Optional[str]:
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _get_http_client_and_config(api_token: Optional[str] = None) -> Any:
    credentials = Credentials.from_token(api_token=_get_token(api_token=api_token))
    http_client, client_config = create_http_client_with_auth(
        credentials=credentials, ssl_verify=ssl_verify(), proxies={}
    )
    return http_client, client_config


def _get_backend_client(api_token: Optional[str] = None) -> SwaggerClientWrapper:
    http_client, client_config = _get_http_client_and_config(api_token)
    client: SwaggerClientWrapper = create_backend_client(client_config=client_config, http_client=http_client)
    return client


def _get_leaderboard_client(api_token: Optional[str] = None) -> SwaggerClientWrapper:
    http_client, client_config = _get_http_client_and_config(api_token)
    client: SwaggerClientWrapper = create_leaderboard_client(client_config=client_config, http_client=http_client)
    return client


def list_api_methods_used_by_client(api_token: Optional[str] = None) -> Dict[str, List[str]]:
    lc = _get_leaderboard_client(api_token)
    bc = _get_backend_client(api_token)

    return {
        "leaderboard_methods": dir(lc.api._api_obj),
        "backend_methods": dir(bc.api._api_obj),
    }
