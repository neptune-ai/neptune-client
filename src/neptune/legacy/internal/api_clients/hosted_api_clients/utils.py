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

import logging
import time

import requests
from bravado.exception import (
    BravadoConnectionError,
    BravadoTimeoutError,
    HTTPBadGateway,
    HTTPForbidden,
    HTTPGatewayTimeout,
    HTTPInternalServerError,
    HTTPRequestTimeout,
    HTTPServiceUnavailable,
    HTTPTooManyRequests,
    HTTPUnauthorized,
)
from urllib3.exceptions import NewConnectionError

from neptune.legacy.api_exceptions import (
    ConnectionLost,
    Forbidden,
    NeptuneSSLVerificationError,
    ServerError,
    Unauthorized,
)

_logger = logging.getLogger(__name__)


def legacy_with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        retries = 11
        retry = 0
        while retry < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.SSLError:
                raise NeptuneSSLVerificationError()
            except HTTPServiceUnavailable:
                if retry >= 6:
                    _logger.warning("Experiencing connection interruptions. Reestablishing communication with Neptune.")
                time.sleep(2**retry)
                retry += 1
                continue
            except (
                BravadoConnectionError,
                BravadoTimeoutError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                HTTPRequestTimeout,
                HTTPGatewayTimeout,
                HTTPBadGateway,
                HTTPInternalServerError,
                NewConnectionError,
            ):
                if retry >= 6:
                    _logger.warning("Experiencing connection interruptions. Reestablishing communication with Neptune.")
                time.sleep(2**retry)
                retry += 1
                continue
            except HTTPTooManyRequests as e:
                response_headers = e.response.json()["headers"]
                if "retry-after" in response_headers:
                    retry_after = int(response_headers["retry-after"][0])
                    time.sleep(retry_after)
                else:
                    time.sleep(2 ** min(10, retry))
                retry += 1
                continue
            except HTTPUnauthorized:
                raise Unauthorized()
            except HTTPForbidden:
                raise Forbidden()
            except requests.exceptions.RequestException as e:
                if e.response is None:
                    raise
                status_code = e.response.status_code
                if status_code in (
                    HTTPRequestTimeout.status_code,
                    HTTPBadGateway.status_code,
                    HTTPServiceUnavailable.status_code,
                    HTTPGatewayTimeout.status_code,
                    HTTPInternalServerError.status_code,
                ):
                    if retry >= 6:
                        _logger.warning(
                            "Experiencing connection interruptions. Reestablishing communication with Neptune."
                        )
                    time.sleep(2**retry)
                    retry += 1
                    continue
                elif status_code == HTTPTooManyRequests.status_code:
                    response_headers = e.response.json()["headers"]
                    if "retry-after" in response_headers:
                        retry_after = int(response_headers["retry-after"][0])
                        time.sleep(retry_after)
                    else:
                        time.sleep(2 ** min(10, retry))
                    retry += 1
                    continue
                elif status_code >= HTTPInternalServerError.status_code:
                    raise ServerError()
                elif status_code == HTTPUnauthorized.status_code:
                    raise Unauthorized()
                elif status_code == HTTPForbidden.status_code:
                    raise Forbidden()
                else:
                    raise
        raise ConnectionLost()

    return wrapper
