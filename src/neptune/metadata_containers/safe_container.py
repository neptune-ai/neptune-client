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
__all__ = ["safety_decorator"]

import functools
import inspect
import os
from typing import Any

from neptune.common.warnings import warn_once
from neptune.envs import NEPTUNE_SAFETY_MODE
from neptune.internal.utils.logger import logger

_SAFETY_MODE = os.getenv(NEPTUNE_SAFETY_MODE, "false").lower() in ("true", "1", "t")


def _safe_function(func: Any) -> Any:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            try:
                warn_once(f"Exception in method {func}: {ex.__class__.__name__}")
                logger.debug("In safe mode exception is ignored", exc_info=True)
            except Exception:
                pass

    return wrapper


def safety_decorator(cls: Any) -> Any:
    if _SAFETY_MODE:
        for name, method in inspect.getmembers(cls):
            if (not inspect.ismethod(method) and not inspect.isfunction(method)) or inspect.isbuiltin(method):
                continue
            setattr(cls, name, _safe_function(method))
        return cls
    else:
        return cls
