#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["with_api_exceptions_handler", "handle_json_errors"]

import itertools
import logging
import os
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
)

import requests
from bravado.exception import (
    BravadoConnectionError,
    BravadoTimeoutError,
    HTTPBadGateway,
    HTTPClientError,
    HTTPForbidden,
    HTTPGatewayTimeout,
    HTTPInternalServerError,
    HTTPRequestTimeout,
    HTTPServiceUnavailable,
    HTTPTooManyRequests,
    HTTPUnauthorized,
)
from requests.exceptions import JSONDecodeError
from urllib3.exceptions import NewConnectionError

from neptune.common.envs import NEPTUNE_RETRIES_TIMEOUT_ENV
from neptune.common.exceptions import (
    ClientHttpError,
    Forbidden,
    NeptuneAuthTokenExpired,
    NeptuneConnectionLostException,
    NeptuneInvalidApiTokenException,
    NeptuneSSLVerificationError,
    Unauthorized,
    WritingToArchivedProjectException,
)

if TYPE_CHECKING:
    from bravado_core.response import IncomingResponse

_logger = logging.getLogger(__name__)

MAX_RETRY_TIME = 30
retries_timeout = int(os.getenv(NEPTUNE_RETRIES_TIMEOUT_ENV, "60"))


def ensure_json_body(response: "IncomingResponse") -> Dict[str, Any]:
    try:
        return response.json() or dict()
    except JSONDecodeError:
        return {}


def handle_json_errors(
    response: "IncomingResponse",
    source_exception: Exception,
    error_processors: Dict[str, Callable[[Dict[str, Any]], Exception]],
    default_exception: Optional[Exception] = None,
) -> None:
    body = ensure_json_body(response)

    error_type: Optional[str] = body.get("errorType")
    error_processor = error_processors.get(error_type)
    if error_processor:
        raise error_processor(body) from source_exception
    elif default_exception:
        raise default_exception from source_exception
    raise source_exception


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        last_exception = None
        start_time = time.monotonic()
        for retry in itertools.count(0):
            if time.monotonic() - start_time > retries_timeout:
                break

            try:
                return func(*args, **kwargs)
            except requests.exceptions.InvalidHeader as e:
                if "X-Neptune-Api-Token" in e.args[0]:
                    raise NeptuneInvalidApiTokenException()
                raise
            except requests.exceptions.SSLError as e:
                raise NeptuneSSLVerificationError() from e
            except (
                BravadoConnectionError,
                BravadoTimeoutError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                HTTPRequestTimeout,
                HTTPServiceUnavailable,
                HTTPGatewayTimeout,
                HTTPBadGateway,
                HTTPTooManyRequests,
                HTTPInternalServerError,
                NewConnectionError,
            ) as e:
                time.sleep(min(2 ** min(10, retry), MAX_RETRY_TIME))
                last_exception = e
                continue
            except NeptuneAuthTokenExpired:
                continue
            except HTTPUnauthorized:
                raise Unauthorized()
            except HTTPForbidden as e:
                handle_json_errors(
                    response=e.response,
                    error_processors={
                        "WRITE_ACCESS_DENIED_TO_ARCHIVED_PROJECT": lambda _: WritingToArchivedProjectException()
                    },
                    source_exception=e,
                    default_exception=Forbidden(),
                )
            except HTTPClientError as e:
                raise ClientHttpError(e.status_code, e.response.text) from e
            except requests.exceptions.RequestException as e:
                if e.response is None:
                    raise
                status_code = e.response.status_code
                if status_code in (
                    HTTPRequestTimeout.status_code,
                    HTTPBadGateway.status_code,
                    HTTPServiceUnavailable.status_code,
                    HTTPGatewayTimeout.status_code,
                    HTTPTooManyRequests.status_code,
                    HTTPInternalServerError.status_code,
                ):
                    time.sleep(min(2 ** min(10, retry), MAX_RETRY_TIME))
                    last_exception = e
                    continue
                elif status_code == HTTPUnauthorized.status_code:
                    raise Unauthorized()
                elif status_code == HTTPForbidden.status_code:
                    raise Forbidden()
                elif 400 <= status_code < 500:
                    raise ClientHttpError(status_code, e.response.text) from e
                else:
                    raise
        raise NeptuneConnectionLostException(last_exception) from last_exception

    return wrapper
