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
import click

from neptune.alpha.internal.backends.utils import (
    check_if_ssl_verify as alpha_check_if_ssl_verify,
    create_http_client as alpha_create_http_client,
    create_swagger_client as alpha_create_swagger_client,
)
from neptune.alpha.internal.credentials import Credentials
from neptune.backend import Backend
from neptune.exceptions import InvalidNeptuneBackend, STYLES
from neptune.internal.backends import (
    AlphaIntegrationBackend,
    HostedNeptuneBackend,
    OfflineBackend,
)


def backend_factory(*, project_qualified_name, backend_name, api_token=None, proxies=None) -> Backend:
    if backend_name == 'offline':
        return OfflineBackend()

    elif backend_name is None:
        credentials = Credentials(api_token)
        ssl_verify = alpha_check_if_ssl_verify()
        boot_http_client = alpha_create_http_client(ssl_verify, proxies)
        config_api_url = credentials.api_url_opt or credentials.token_origin_address
        boot_backend_client = alpha_create_swagger_client(f'{config_api_url}/api/backend/swagger.json',
                                                          boot_http_client)

        response = boot_backend_client.api.getProject(projectIdentifier=project_qualified_name).response()
        warning = response.metadata.headers.get('X-Server-Warning')
        if warning:
            click.echo('{warning}{content}{end}'.format(content=warning, **STYLES))
        project = response.result
        if not hasattr(project, 'version'):
            pass  # what now?

        if project.version == 1:
            return HostedNeptuneBackend(api_token, proxies)
        else:
            return AlphaIntegrationBackend(api_token, proxies)

    else:
        raise InvalidNeptuneBackend(backend_name)
