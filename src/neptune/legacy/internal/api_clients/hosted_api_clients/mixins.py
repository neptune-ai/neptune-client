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
import logging
import socket
import sys

import click
from bravado.client import SwaggerClient
from bravado_core.formatter import SwaggerFormat
from packaging import version
from six.moves import urllib

from neptune.legacy.exceptions import (
    CannotResolveHostname,
    DeprecatedApiToken,
    UnsupportedClientVersion,
)
from neptune.legacy.internal.api_clients.client_config import (
    ClientConfig,
    MultipartConfig,
)
from neptune.legacy.internal.api_clients.hosted_api_clients.utils import legacy_with_api_exceptions_handler

_logger = logging.getLogger(__name__)

uuid_format = SwaggerFormat(
    format="uuid",
    to_python=lambda x: x,
    to_wire=lambda x: x,
    validate=lambda x: None,
    description="",
)


class HostedNeptuneMixin:
    """Mixin containing operation common for both backend and leaderboard api clients"""

    @legacy_with_api_exceptions_handler
    def _get_swagger_client(self, url, http_client):
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

    @staticmethod
    def _get_client_config_args(api_token):
        return dict(X_Neptune_Api_Token=api_token)

    @legacy_with_api_exceptions_handler
    def _create_client_config(self, api_token, backend_client):
        client_config_args = self._get_client_config_args(api_token)
        config = backend_client.api.getClientConfig(**client_config_args).response().result

        if hasattr(config, "pyLibVersions"):
            min_recommended = getattr(config.pyLibVersions, "minRecommendedVersion", None)
            min_compatible = getattr(config.pyLibVersions, "minCompatibleVersion", None)
            max_compatible = getattr(config.pyLibVersions, "maxCompatibleVersion", None)
        else:
            click.echo(
                "ERROR: This client version is not supported by your Neptune instance. Please contant Neptune support.",
                sys.stderr,
            )
            raise UnsupportedClientVersion(self.client_lib_version, None, "0.4.111")

        multipart_upload_config_obj = getattr(config, "multiPartUpload", None)
        has_multipart_upload = getattr(multipart_upload_config_obj, "enabled", False)
        if not has_multipart_upload:
            multipart_upload_config = None
        else:
            min_chunk_size = getattr(multipart_upload_config_obj, "minChunkSize")
            max_chunk_size = getattr(multipart_upload_config_obj, "maxChunkSize")
            max_chunk_count = getattr(multipart_upload_config_obj, "maxChunkCount")
            max_single_part_size = getattr(multipart_upload_config_obj, "maxSinglePartSize")
            multipart_upload_config = MultipartConfig(
                min_chunk_size, max_chunk_size, max_chunk_count, max_single_part_size
            )

        return ClientConfig(
            api_url=config.apiUrl,
            display_url=config.applicationUrl,
            min_recommended_version=version.parse(min_recommended) if min_recommended else None,
            min_compatible_version=version.parse(min_compatible) if min_compatible else None,
            max_compatible_version=version.parse(max_compatible) if max_compatible else None,
            multipart_config=multipart_upload_config,
        )

    def _verify_host_resolution(self, api_url, app_url):
        host = urllib.parse.urlparse(api_url).netloc.split(":")[0]
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            if self.credentials.api_url_opt is None:
                raise DeprecatedApiToken(urllib.parse.urlparse(app_url).netloc)
            else:
                raise CannotResolveHostname(host)
