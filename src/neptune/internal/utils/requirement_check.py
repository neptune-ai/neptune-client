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
__all__ = [
    "require_installed",
    "is_installed",
]

from functools import lru_cache
from importlib.util import find_spec
from typing import Optional

from neptune.exceptions import NeptuneMissingRequirementException


@lru_cache(maxsize=32)
def is_installed(requirement_name: str) -> bool:
    return find_spec(requirement_name) is not None


def require_installed(requirement_name: str, *, suggestion: Optional[str] = None) -> None:
    if is_installed(requirement_name):
        return

    raise NeptuneMissingRequirementException(requirement_name, suggestion)
