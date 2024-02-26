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
__all__ = ["with_api_exceptions_handler", "get_retry_from_headers_or_default"]

import itertools
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
from bravado_core.util import RecursiveCallException
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
from neptune.common.utils import reset_internal_ssl_state
from neptune.internal.utils.logger import get_logger

_logger = get_logger()

MAX_RETRY_TIME = 30
MAX_RETRY_MULTIPLIER = 10
retries_timeout = int(os.getenv(NEPTUNE_RETRIES_TIMEOUT_ENV, "60"))


def get_retry_from_headers_or_default(headers, retry_count):
    try:
        return (
            int(headers["retry-after"][0]) if "retry-after" in headers else 2 ** min(MAX_RETRY_MULTIPLIER, retry_count)
        )
    except Exception:
        return min(2 ** min(MAX_RETRY_MULTIPLIER, retry_count), MAX_RETRY_TIME)


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        ssl_error_occurred = False
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
                """
                OpenSSL's internal random number generator does not properly handle forked processes.
                Applications must change the PRNG state of the parent process
                if they use any SSL feature with os.fork().
                Any successful call of RAND_add(), RAND_bytes() or RAND_pseudo_bytes() is sufficient.
                https://docs.python.org/3/library/ssl.html#multi-processing
                On Linux it looks like it does not help much but does not break anything either.
                But single retry seems to solve the issue.
                """
                if not ssl_error_occurred:
                    ssl_error_occurred = True
                    reset_internal_ssl_state()
                    continue

                if "CertificateError" in str(e.__context__):
                    raise NeptuneSSLVerificationError() from e
                else:
                    time.sleep(min(2 ** min(MAX_RETRY_MULTIPLIER, retry), MAX_RETRY_TIME))
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
                HTTPInternalServerError,
                NewConnectionError,
                ChunkedEncodingError,
                RecursiveCallException,
            ) as e:
                time.sleep(min(2 ** min(MAX_RETRY_MULTIPLIER, retry), MAX_RETRY_TIME))
                last_exception = e
                continue
            except HTTPTooManyRequests as e:
                wait_time = get_retry_from_headers_or_default(e.response.headers, retry)
                time.sleep(wait_time)
                last_exception = e
                continue
            except NeptuneAuthTokenExpired as e:
                last_exception = e
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
                    HTTPInternalServerError.status_code,
                ):
                    time.sleep(min(2 ** min(MAX_RETRY_MULTIPLIER, retry), MAX_RETRY_TIME))
                    last_exception = e
                    continue
                elif status_code == HTTPTooManyRequests.status_code:
                    wait_time = get_retry_from_headers_or_default(e.response.headers, retry)
                    time.sleep(wait_time)
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
