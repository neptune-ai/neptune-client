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

from __future__ import annotations

__all__ = ("Evaluable", "trigger_evaluation", "noop_if_not_triggered")

import abc
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)

RT = TypeVar("RT")


class Evaluable(abc.ABC):
    @abc.abstractmethod
    def evaluate(self) -> None: ...

    @property
    @abc.abstractmethod
    def evaluated(self) -> bool: ...


def trigger_evaluation(method: Callable[..., RT]) -> Callable[..., RT]:
    def _wrapper(obj: Evaluable, *args: Any, **kwargs: Any) -> RT:
        if not obj.evaluated:
            obj.evaluate()
        return method(obj, *args, **kwargs)

    return _wrapper


def noop_if_not_triggered(method: Callable[..., RT]) -> Callable[..., Optional[RT]]:
    def _wrapper(obj: Evaluable, *args: Any, **kwargs: Any) -> Optional[RT]:
        if obj.evaluated:
            return method(obj, *args, **kwargs)
        return None

    return _wrapper
