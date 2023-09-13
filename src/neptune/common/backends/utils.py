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
__all__ = ["with_api_exceptions_handler"]

import itertools
import logging
import os
import time

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
from requests.exceptions import ChunkedEncodingError
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
)

_logger = logging.getLogger(__name__)

MAX_RETRY_TIME = 30
retries_timeout = int(os.getenv(NEPTUNE_RETRIES_TIMEOUT_ENV, "60"))


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        last_exception = None
        start_time = time.monotonic()
        for retry in itertools.count(0):
            if time.monotonic() - start_time > retries_timeout:
                break

            try:
                before = time.monotonic()
                ret = func(*args, **kwargs)
                after = time.monotonic()
                if after - before > 50:
                    _logger.warning(f"WARNING: Request succeeded after {int(after - before)} seconds.")
                return ret
            except requests.exceptions.InvalidHeader as e:
                if "X-Neptune-Api-Token" in e.args[0]:
                    raise NeptuneInvalidApiTokenException()
                raise
            except requests.exceptions.SSLError as e:
                _logger.error("CONNECTION ERROR: SSLError")
                time.sleep(min(2 ** min(10, retry), MAX_RETRY_TIME))
                last_exception = e
                continue
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
                ChunkedEncodingError,
            ) as e:
                try:
                    _logger.error(f"CONNECTION ERROR: {e.status_code}")
                except AttributeError:
                    _logger.error(f"CONNECTION ERROR: {type(e).__name__}")
                time.sleep(min(2 ** min(10, retry), MAX_RETRY_TIME))
                last_exception = e
                continue
            except NeptuneAuthTokenExpired:
                _logger.exception("TOKEN EXPIRED")
                continue
            except HTTPUnauthorized:
                raise Unauthorized()
            except HTTPForbidden:
                raise Forbidden()
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
                    _logger.error(f"CONNECTION ERROR: {status_code}")
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
        try:
            raise last_exception
        except requests.exceptions.SSLError as e:
            raise NeptuneSSLVerificationError() from e
        except Exception:
            raise NeptuneConnectionLostException(last_exception) from last_exception

    return wrapper
