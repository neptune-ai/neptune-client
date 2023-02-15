#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import os
import platform
import sys

import click
import urllib3
from bravado.exception import HTTPNotFound
from bravado.requests_client import RequestsClient
from packaging import version

from neptune.common.exceptions import STYLES
from neptune.common.oauth import NeptuneAuthenticator
from neptune.common.utils import (
    NoopObject,
    update_session_proxies,
)
from neptune.internal.backends.hosted_client import NeptuneResponseAdapter
from neptune.legacy.api_exceptions import (
    ProjectNotFound,
    WorkspaceNotFound,
)
from neptune.legacy.backend import (
    BackendApiClient,
    LeaderboardApiClient,
)
from neptune.legacy.exceptions import UnsupportedClientVersion
from neptune.legacy.internal.api_clients.credentials import Credentials
from neptune.legacy.internal.api_clients.hosted_api_clients.hosted_alpha_leaderboard_api_client import (
    HostedAlphaLeaderboardApiClient,
)
from neptune.legacy.internal.api_clients.hosted_api_clients.mixins import HostedNeptuneMixin
from neptune.legacy.internal.api_clients.hosted_api_clients.utils import legacy_with_api_exceptions_handler
from neptune.legacy.projects import Project

_logger = logging.getLogger(__name__)


class HostedNeptuneBackendApiClient(HostedNeptuneMixin, BackendApiClient):
    @legacy_with_api_exceptions_handler
    def __init__(self, api_token=None, proxies=None):
        self._old_leaderboard_client = None
        self._new_leaderboard_client = None
        self._api_token = api_token
        self._proxies = proxies

        # This is not a top-level import because of circular dependencies
        from neptune import __version__

        self.client_lib_version = __version__

        self.credentials = Credentials(api_token)

        ssl_verify = True
        if os.getenv("NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = RequestsClient(ssl_verify=ssl_verify, response_adapter_class=NeptuneResponseAdapter)
        # for session re-creation we need to keep an authenticator-free version of http client
        self._http_client_for_token = RequestsClient(
            ssl_verify=ssl_verify, response_adapter_class=NeptuneResponseAdapter
        )

        user_agent = "neptune-client/{lib_version} ({system}, python {python_version})".format(
            lib_version=self.client_lib_version,
            system=platform.platform(),
            python_version=platform.python_version(),
        )
        self.http_client.session.headers.update({"User-Agent": user_agent})
        self._http_client_for_token.session.headers.update({"User-Agent": user_agent})

        update_session_proxies(self.http_client.session, proxies)
        update_session_proxies(self._http_client_for_token.session, proxies)

        config_api_url = self.credentials.api_url_opt or self.credentials.token_origin_address
        # We don't need to be able to resolve Neptune host if we use proxy
        if proxies is None:
            self._verify_host_resolution(config_api_url, self.credentials.token_origin_address)

        # this backend client is used only for initial configuration and session re-creation
        self.backend_client = self._get_swagger_client(
            "{}/api/backend/swagger.json".format(config_api_url),
            self._http_client_for_token,
        )
        self._client_config = self._create_client_config(
            api_token=self.credentials.api_token, backend_client=self.backend_client
        )

        self._verify_version()

        self.backend_swagger_client = self._get_swagger_client(
            "{}/api/backend/swagger.json".format(self._client_config.api_url),
            self.http_client,
        )

        self.authenticator = self._create_authenticator(
            api_token=self.credentials.api_token,
            ssl_verify=ssl_verify,
            proxies=proxies,
            backend_client=self.backend_client,
        )
        self.http_client.authenticator = self.authenticator

        if sys.version_info >= (3, 7):
            try:
                os.register_at_fork(after_in_child=self._handle_fork_in_child)
            except AttributeError:
                pass

    def _handle_fork_in_child(self):
        self.backend_swagger_client = NoopObject()

    @property
    def api_address(self):
        return self._client_config.api_url

    @property
    def http_client(self):
        return self._http_client

    @property
    def display_address(self):
        return self._client_config.display_url

    @property
    def proxies(self):
        return self._proxies

    @legacy_with_api_exceptions_handler
    def get_project(self, project_qualified_name):
        try:
            response = self.backend_swagger_client.api.getProject(projectIdentifier=project_qualified_name).response()
            warning = response.metadata.headers.get("X-Server-Warning")
            if warning:
                click.echo("{warning}{content}{end}".format(content=warning, **STYLES))
            project = response.result

            return Project(
                backend=self.create_leaderboard_backend(project=project),
                internal_id=project.id,
                namespace=project.organizationName,
                name=project.name,
            )
        except HTTPNotFound:
            raise ProjectNotFound(project_qualified_name)

    @legacy_with_api_exceptions_handler
    def get_projects(self, namespace):
        try:
            r = self.backend_swagger_client.api.listProjects(organizationIdentifier=namespace).response()
            return r.result.entries
        except HTTPNotFound:
            raise WorkspaceNotFound(namespace_name=namespace)

    def create_leaderboard_backend(self, project) -> LeaderboardApiClient:
        return self.get_new_leaderboard_client()

    def get_new_leaderboard_client(self) -> HostedAlphaLeaderboardApiClient:
        if self._new_leaderboard_client is None:
            self._new_leaderboard_client = HostedAlphaLeaderboardApiClient(backend_api_client=self)
        return self._new_leaderboard_client

    @legacy_with_api_exceptions_handler
    def _create_authenticator(self, api_token, ssl_verify, proxies, backend_client):
        return NeptuneAuthenticator(api_token, backend_client, ssl_verify, proxies)

    def _verify_version(self):
        parsed_version = version.parse(self.client_lib_version)

        if self._client_config.min_compatible_version and self._client_config.min_compatible_version > parsed_version:
            click.echo(
                "ERROR: Minimal supported client version is {} (installed: {}). Please upgrade neptune-client".format(
                    self._client_config.min_compatible_version, self.client_lib_version
                ),
                sys.stderr,
            )
            raise UnsupportedClientVersion(
                self.client_lib_version,
                self._client_config.min_compatible_version,
                self._client_config.max_compatible_version,
            )
        if self._client_config.max_compatible_version and self._client_config.max_compatible_version < parsed_version:
            click.echo(
                "ERROR: Maximal supported client version is {} (installed: {}). Please downgrade neptune-client".format(
                    self._client_config.max_compatible_version, self.client_lib_version
                ),
                sys.stderr,
            )
            raise UnsupportedClientVersion(
                self.client_lib_version,
                self._client_config.min_compatible_version,
                self._client_config.max_compatible_version,
            )
        if self._client_config.min_recommended_version and self._client_config.min_recommended_version > parsed_version:
            click.echo(
                "WARNING: We recommend an upgrade to a new version of neptune-client - {} (installed - {}).".format(
                    self._client_config.min_recommended_version, self.client_lib_version
                ),
                sys.stderr,
            )
