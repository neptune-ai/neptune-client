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
from typing import Tuple

import click

from neptune.alpha.internal.backends.utils import (
    check_if_ssl_verify as alpha_check_if_ssl_verify,
    create_http_client as alpha_create_http_client,
    create_swagger_client as alpha_create_swagger_client,
)
from neptune.alpha.internal.credentials import Credentials
from neptune.backend import Backend
from neptune.exceptions import STYLES
from neptune.internal.backends import (
    AlphaIntegrationBackend,
    HostedNeptuneBackend,
)
from neptune.internal.utils.http import verify_host_resolution
from neptune.projects import Project

_logger = logging.getLogger(__name__)


def get_token_backend_client(api_token, proxies=None):
    credentials = Credentials(api_token)
    ssl_verify = alpha_check_if_ssl_verify()
    token_http_client = alpha_create_http_client(ssl_verify, proxies)
    config_api_url = credentials.api_url_opt or credentials.token_origin_address
    if proxies is None:
        verify_host_resolution(
            api_url=config_api_url,
            app_url=credentials.token_origin_address,
            api_url_opt=credentials.api_url_opt
        )
    return alpha_create_swagger_client(f'{config_api_url}/api/backend/swagger.json', token_http_client)


def backend_initializer(*, project_qualified_name, api_token=None, proxies=None) -> Tuple[Backend, Project]:
    """Return initialized `HostedNeptuneBackend` of `AlphaIntegrationBackend` depending on the api_project version.
    Function additionally returns api_project to avoid duplicated API call.
    """
    token_backend_client = get_token_backend_client(api_token, proxies)

    response = token_backend_client.api.getProject(projectIdentifier=project_qualified_name).response()
    warning = response.metadata.headers.get('X-Server-Warning')
    if warning:
        click.echo('{warning}{content}{end}'.format(content=warning, **STYLES))
    api_project = response.result

    if not hasattr(api_project, 'version') or api_project.version == 1:
        backend = HostedNeptuneBackend(api_token, proxies)
    elif api_project.version == 2:
        backend = AlphaIntegrationBackend(token_backend_client, api_token, proxies)
    else:
        _logger.warning(f'Unknown api_project version: {api_project.version}. Assuming v2 api_project.')
        return AlphaIntegrationBackend(token_backend_client, api_token, proxies), api_project

    project = Project(
        backend=backend,
        internal_id=api_project.id,
        namespace=api_project.organizationName,
        name=api_project.name)
    return backend, project
