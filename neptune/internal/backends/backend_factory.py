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
from neptune.exceptions import InvalidNeptuneBackend
from neptune.backend import Backend
from neptune.internal.backends import (
    AlphaIntegrationBackend,
    HostedNeptuneBackend,
    OfflineBackend,
)


def backend_factory(*, backend_name, api_token=None, proxies=None) -> Backend:
    if backend_name == 'offline':
        return OfflineBackend()

    elif backend_name is None:
        credentials = Credentials(api_token)
        # TODO: Improvement. How to determine which backend class should be used?
        if credentials.token_origin_address.startswith('https://alpha.'):
            return AlphaIntegrationBackend(api_token, proxies)

        return HostedNeptuneBackend(api_token, proxies)

    else:
        raise InvalidNeptuneBackend(backend_name)
