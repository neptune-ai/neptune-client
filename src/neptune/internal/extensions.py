#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["load_extensions"]

import sys
from typing import (
    Callable,
    List,
    Tuple,
)

if sys.version_info >= (3, 8):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)


def get_entry_points(name: str) -> List[Tuple[str, Callable[[], None]]]:
    if (3, 8) <= sys.version_info < (3, 10):
        return [(entry_point.name, entry_point.load()) for entry_point in entry_points().get(name, tuple())]
    return [
        (entry_point.name, entry_point.load())  # type: ignore[unused-ignore, attr-defined]
        for entry_point in entry_points(group=name)  # type: ignore[unused-ignore, call-arg]
    ]


def load_extensions() -> None:
    for entry_point_name, loaded_extension in get_entry_points(name="neptune.extensions"):
        try:
            _ = loaded_extension()
        except Exception as e:
            warn_once(
                message=f"Failed to load neptune extension `{entry_point_name}` with exception: {e}",
                exception=NeptuneWarning,
            )
