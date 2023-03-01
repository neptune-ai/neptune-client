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
__all__ = ["Assignable"]

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neptune.handler import Handler


class Assignable(abc.ABC):
    """
    Interface for objects that metadata can be assigned to.

    This could be a run, model, model version or project or already selected namespace that metadata
    will be stored under.

    Example:
        >>> from neptune import init_run
        >>> from neptune.typing import Assignable
        >>> class NeptuneCallback:
        ...     # Proper type hinting of `start_from` parameter.
        ...     def __init__(self, start_from: Assignable):
        ...         self._start_from = start_from
        ...
        ...     def log_accuracy(self, accuracy: float) -> None:
        ...         self._start_from["train/acc"] = accuracy
        ...
        >>> run = init_run()
        >>> callback = NeptuneCallback(start_from=run)
        >>> callback.log_accuracy(0.8)
        >>> # or
        ... callback = NeptuneCallback(start_from=run["some/random/path"])
        >>> callback.log_accuracy(0.8)
    """

    @abc.abstractmethod
    def __getitem__(self, path: str) -> "Handler":
        raise NotImplementedError

    @abc.abstractmethod
    def __setitem__(self, key: str, value) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def __delitem__(self, path) -> None:
        raise NotImplementedError
