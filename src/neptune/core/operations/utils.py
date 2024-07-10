#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

from typing import Optional

from neptune.core.operations.operation import (
    LogFloats,
    Operation,
)


def try_get_step(operation: Operation) -> Optional[float]:
    if isinstance(operation, LogFloats):
        # assumtion: there is at most one value in the LogFloats values list
        return operation.values[0].step if operation.values else None
    else:
        return None
