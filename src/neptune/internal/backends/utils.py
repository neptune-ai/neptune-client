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
__all__ = [
    "verify_host_resolution",
    "create_swagger_client",
    "verify_client_version",
    "update_session_proxies",
    "build_operation_url",
    "handle_server_raw_response_messages",
    "NeptuneResponseAdapter",
    "MissingApiClient",
    "cache",
    "ssl_verify",
    "parse_validation_errors",
    "ExecuteOperationsBatchingManager",
    "which_progress_bar",
    "construct_progress_bar",
    "with_api_exceptions_handler",
]

import dataclasses
import itertools
import os
import socket
import time
from functools import (
    lru_cache,
    wraps,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Text,
    Type,
)
from urllib.parse import (
    urljoin,
    urlparse,
)

import requests
import urllib3
from bravado.client import SwaggerClient
from bravado.exception import (
    BravadoConnectionError,
    BravadoTimeoutError,
    HTTPBadGateway,
    HTTPClientError,
    HTTPError,
    HTTPForbidden,
    HTTPGatewayTimeout,
    HTTPInternalServerError,
    HTTPRequestTimeout,
    HTTPServiceUnavailable,
    HTTPTooManyRequests,
    HTTPUnauthorized,
)
from bravado.http_client import HttpClient
from bravado.requests_client import RequestsResponseAdapter
from bravado_core.formatter import SwaggerFormat
from bravado_core.util import RecursiveCallException
from packaging.version import Version
from requests import (
    Response,
    Session,
)
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import NewConnectionError

from neptune.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE
from neptune.exceptions import (
    CannotResolveHostname,
    MetadataInconsistency,
    NeptuneClientUpgradeRequiredError,
    NeptuneFeatureNotAvailableException,
)
from neptune.internal.backends.api_model import ClientConfig
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.internal.envs import NEPTUNE_RETRIES_TIMEOUT_ENV
from neptune.internal.exceptions import (
    ClientHttpError,
    Forbidden,
    NeptuneAuthTokenExpired,
    NeptuneConnectionLostException,
    NeptuneInvalidApiTokenException,
    NeptuneSSLVerificationError,
    Unauthorized,
)
from neptune.internal.operation import (
    CopyAttribute,
    Operation,
)
from neptune.internal.utils import replace_patch_version
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.utils import reset_internal_ssl_state
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.typing import (
    ProgressBarCallback,
    ProgressBarType,
)
from neptune.utils import (
    NullProgressBar,
    TqdmProgressBar,
)

logger = get_logger()

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


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


@lru_cache(maxsize=None, typed=True)
def verify_host_resolution(url: str) -> None:
    host = urlparse(url).netloc.split(":")[0]
    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        raise CannotResolveHostname(host)


uuid_format = SwaggerFormat(
    format="uuid",
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description="",
)


@with_api_exceptions_handler
def create_swagger_client(url: str, http_client: HttpClient) -> SwaggerClient:
    return SwaggerClient.from_url(
        url,
        config=dict(
            validate_swagger_spec=False,
            validate_requests=False,
            validate_responses=False,
            formats=[uuid_format],
        ),
        http_client=http_client,
    )


def verify_client_version(client_config: ClientConfig, version: Version):
    base_version = Version(f"{version.major}.{version.minor}.{version.micro}")
    version_with_patch_0 = Version(replace_patch_version(str(version)))

    min_compatible = client_config.version_info.min_compatible
    max_compatible = client_config.version_info.max_compatible
    min_recommended = client_config.version_info.min_recommended

    if min_compatible and min_compatible > base_version:
        raise NeptuneClientUpgradeRequiredError(version, min_version=client_config.version_info.min_compatible)

    if max_compatible and max_compatible < version_with_patch_0:
        raise NeptuneClientUpgradeRequiredError(version, max_version=client_config.version_info.max_compatible)

    if min_recommended and min_recommended > version:
        logger.warning(
            "WARNING: Your version of the Neptune client library (%s) is deprecated,"
            " and soon will no longer be supported by the Neptune server."
            " We recommend upgrading to at least version %s.",
            version,
            min_recommended,
        )


def update_session_proxies(session: Session, proxies: Optional[Dict[str, str]]):
    if proxies:
        try:
            session.proxies.update(proxies)
        except (TypeError, ValueError):
            raise ValueError(f"Wrong proxies format: {proxies}")


def build_operation_url(base_api: str, operation_url: str) -> str:
    if "://" not in base_api:
        base_api = f"https://{base_api}"

    return urljoin(base=base_api, url=operation_url)


# TODO print in color once colored exceptions are added
def handle_server_raw_response_messages(response: Response):
    try:
        info = response.headers.get("X-Server-Info")
        if info:
            logger.info(info)
        warning = response.headers.get("X-Server-Warning")
        if warning:
            logger.warning(warning)
        error = response.headers.get("X-Server-Error")
        if error:
            logger.error(error)
        return response
    except Exception:
        # any issues with printing server messages should not cause code to fail
        return response


# TODO print in color once colored exceptions are added
class NeptuneResponseAdapter(RequestsResponseAdapter):
    @property
    def raw_bytes(self) -> bytes:
        self._handle_response()
        return super().raw_bytes

    @property
    def text(self) -> Text:
        self._handle_response()
        return super().text

    def json(self, **kwargs) -> Mapping[Text, Any]:
        self._handle_response()
        return super().json(**kwargs)

    def _handle_response(self):
        try:
            info = self._delegate.headers.get("X-Server-Info")
            if info:
                logger.info(info)
            warning = self._delegate.headers.get("X-Server-Warning")
            if warning:
                logger.warning(warning)
            error = self._delegate.headers.get("X-Server-Error")
            if error:
                logger.error(error)
        except Exception:
            # any issues with printing server messages should not cause code to fail
            pass


class MissingApiClient(SwaggerClientWrapper):
    """catch-all class to gracefully handle calls to unavailable API"""

    def __init__(self, feature_name: str):
        self.feature_name = feature_name

    def __getattr__(self, item):
        raise NeptuneFeatureNotAvailableException(missing_feature=self.feature_name)


# https://stackoverflow.com/a/44776960
def cache(func):
    """
    Transform mutable dictionary into immutable before call to lru_cache
    """

    class HDict(dict):
        def __hash__(self):
            return hash(frozenset(self.items()))

    func = lru_cache(maxsize=None, typed=True)(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        args = tuple([HDict(arg) if isinstance(arg, dict) else arg for arg in args])
        kwargs = {k: HDict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    wrapper.cache_clear = func.cache_clear
    return wrapper


def ssl_verify():
    if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
        urllib3.disable_warnings()
        return False

    return True


def parse_validation_errors(error: HTTPError) -> Dict[str, str]:
    return {
        f"{error_description.get('errorCode').get('name')}": error_description.get("context", "")
        for validation_error in error.swagger_result.validationErrors
        for error_description in validation_error.get("errors")
    }


@dataclasses.dataclass
class OperationsBatch:
    operations: List[Operation] = dataclasses.field(default_factory=list)
    errors: List[MetadataInconsistency] = dataclasses.field(default_factory=list)
    dropped_operations_count: int = 0


class ExecuteOperationsBatchingManager:
    def __init__(self, backend: "NeptuneBackend"):
        self._backend = backend

    def get_batch(self, ops: Iterable[Operation]) -> OperationsBatch:
        result = OperationsBatch()
        for op in ops:
            if isinstance(op, CopyAttribute):
                if not result.operations:
                    try:
                        # CopyAttribute can be at the start of a batch
                        result.operations.append(op.resolve(self._backend))
                    except MetadataInconsistency as e:
                        result.errors.append(e)
                        result.dropped_operations_count += 1
                else:
                    # cannot have CopyAttribute after any other op in a batch
                    break
            else:
                result.operations.append(op)

        return result


def _check_if_tqdm_installed() -> bool:
    try:
        import tqdm  # noqa: F401

        return True
    except ImportError:  # tqdm not installed
        return False


def which_progress_bar(progress_bar: Optional[ProgressBarType]) -> Type[ProgressBarCallback]:
    if isinstance(progress_bar, type) and issubclass(
        progress_bar, ProgressBarCallback
    ):  # return whatever the user gave us
        return progress_bar

    if not isinstance(progress_bar, bool) and progress_bar is not None:
        raise TypeError(f"progress_bar should be None, bool or ProgressBarCallback, got {type(progress_bar).__name__}")

    if progress_bar or progress_bar is None:
        tqdm_available = _check_if_tqdm_installed()

        if not tqdm_available:
            warn_once(
                "To use the default progress bar, please install tqdm: pip install tqdm",
                exception=NeptuneWarning,
            )
            return NullProgressBar
        return TqdmProgressBar

    return NullProgressBar


def construct_progress_bar(
    progress_bar: Optional[ProgressBarType],
    description: str,
) -> ProgressBarCallback:
    progress_bar_type = which_progress_bar(progress_bar)
    return progress_bar_type(description=description)
