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

if sys.version_info >= (3, 8):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)


def load_extensions() -> None:
    for entry_point in entry_points().get("neptune.extensions") or tuple():
        try:
            loaded_extension = entry_point.load()
            _ = loaded_extension()
        except Exception as e:
            warn_once(
                message=f"Failed to load neptune extension `{entry_point.name}` with exception: {e}",
                exception=NeptuneWarning,
            )
