#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import uuid

from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.leaderboard import Leaderboard


class Project:

    def __init__(self,
                 _uuid: uuid.UUID,
                 backend: HostedNeptuneBackend):
        self._uuid = _uuid
        self._backend = backend

    def get_table(self, page: int = 0, page_size: int = 100) -> Leaderboard:
        if page < 0:
            raise ValueError("Page must be 0 or greater.")
        if page_size < 1:
            raise ValueError("Page size must greater than 0.")

        leaderboard = self._backend.get_leaderboard(self._uuid, page * page_size, page_size)
        return Leaderboard(leaderboard.experiments, leaderboard.total_experiments)
