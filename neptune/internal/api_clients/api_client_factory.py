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

from neptune.alpha.internal.credentials import Credentials
from neptune.exceptions import InvalidNeptuneApiClient
from neptune.api_client import ApiClient
from neptune.internal.api_clients import (
    AlphaIntegrationApiClient,
    HostedNeptuneApiClient,
    OfflineApiClient,
)


def api_client_factory(*, api_client_name, api_token=None, proxies=None) -> ApiClient:
    if api_client_name == 'offline':
        return OfflineApiClient()

    elif api_client_name is None:
        credentials = Credentials(api_token)
        # TODO: Improvement. How to determine which api_client class should be used?
        if credentials.token_origin_address.startswith('https://alpha.'):
            return AlphaIntegrationApiClient(api_token, proxies)

        return HostedNeptuneApiClient(api_token, proxies)

    else:
        raise InvalidNeptuneApiClient(api_client_name)
