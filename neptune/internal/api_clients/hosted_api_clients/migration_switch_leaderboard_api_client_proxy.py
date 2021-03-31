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
import threading

from neptune.backend import LeaderboardApiClient

from neptune.exceptions import ProjectMigratedToNewStructure
from neptune.internal.api_clients import HostedNeptuneBackendApiClient
from neptune.internal.api_clients.hosted_api_clients.hosted_leaderboard_api_client import \
    HostedNeptuneLeaderboardApiClient

_logger = logging.getLogger(__name__)


# pylint: disable=abstract-method
class MigrationSwitchLeaderboardApiClientProxy(LeaderboardApiClient):

    def __init__(self, api_client: HostedNeptuneLeaderboardApiClient, backend_client: HostedNeptuneBackendApiClient):
        self._client = api_client
        self._backend_client = backend_client

        self._lock = threading.RLock()
        self._switched = False

    def __getattr__(self, item):
        def func(*args, **kwargs):
            try:
                return getattr(self._client, item)(*args, **kwargs)
            except ProjectMigratedToNewStructure:
                if not self._switched:
                    self._lock.acquire()
                    if not self._switched:
                        self._client = self._backend_client.get_new_leaderboard_client()
                        self._switched = True
                    self._lock.release()
                return getattr(self._client, item)
        return func
