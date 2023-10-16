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
__all__ = ["safe_function"]

import functools
import os
from typing import (
    Any,
    Dict,
    Tuple,
)

from neptune.common.warnings import warn_once
from neptune.envs import NEPTUNE_SAFETY_MODE

_SAFETY_MODE = os.getenv(NEPTUNE_SAFETY_MODE, "false").lower() in ("true", "1", "t")


def safe_function(default_return_value: Any = None) -> Any:
    def decorator(func: Any) -> Any:
        if _SAFETY_MODE:

            @functools.wraps(func)
            def wrapper(*args: Tuple, **kwargs: Dict[str, Any]) -> Any:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    try:
                        warn_once(f"Exception in method {func}: {ex.__class__.__name__}")
                    except Exception:
                        pass
                    return default_return_value

            return wrapper
        else:
            return func

    return decorator
