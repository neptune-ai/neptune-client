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
__all__ = ["RunState"]

import enum

from neptune.common.exceptions import NeptuneException


class RunState(enum.Enum):
    active = "Active"
    inactive = "Inactive"

    _api_active = "running"
    _api_inactive = "idle"

    @staticmethod
    def from_api(value: str) -> "RunState":
        if value == RunState._api_active.value:
            return RunState.active
        elif value == RunState._api_inactive.value:
            return RunState.inactive
        else:
            raise NeptuneException(f"Unknown RunState: {value}")
