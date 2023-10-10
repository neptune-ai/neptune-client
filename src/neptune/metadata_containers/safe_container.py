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
__all__ = ["decorator_safety"]

import functools
from typing import (
    Any,
    Callable,
)

from neptune.internal.utils.logger import logger
import inspect


def _safe_function(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.exception(f"Exception in method ")

    return wrapper


def decorator_safety(cls):
    for name, method in inspect.getmembers(cls):
        if (not inspect.ismethod(method) and not inspect.isfunction(method)) or inspect.isbuiltin(method):
            continue
        print("Decorating function %s" % name)
        setattr(cls, name, _safe_function(method))
    return cls
