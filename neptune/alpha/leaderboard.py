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
from typing import List


class Leaderboard:

    def __init__(self,
                 _experiments: List[uuid.UUID],
                 _total_experiments: int):
        self._experiments = _experiments
        self._total_experiments = _total_experiments

    @property
    def experiments(self) -> List[uuid.UUID]:
        return self._experiments

    @property
    def total_experiments(self) -> int:
        return self._total_experiments
