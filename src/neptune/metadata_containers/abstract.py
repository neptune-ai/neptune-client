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
__all__ = ["SupportsNamespaces", "NeptuneObject", "NeptuneObjectCallback"]

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    Union,
)

if TYPE_CHECKING:
    from neptune.handler import Handler


class SupportsNamespaces(ABC):
    """
    Interface for Neptune objects that supports subscripting (selecting namespaces)
    It could be a Run, Model, ModelVersion, Project or already selected namespace (Handler).

    Example:
        >>> from neptune import init_run
        >>> from neptune.typing import SupportsNamespaces
        >>> class NeptuneCallback:
        ...     # Proper type hinting of `start_from` parameter.
        ...     def __init__(self, start_from: SupportsNamespaces):
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

    @abstractmethod
    def __getitem__(self, path: str) -> "Handler":
        ...

    @abstractmethod
    def __setitem__(self, key: str, value) -> None:
        ...

    @abstractmethod
    def __delitem__(self, path) -> None:
        ...

    @abstractmethod
    def get_root_object(self) -> "SupportsNamespaces":
        ...


class NeptuneObject(SupportsNamespaces, ABC):
    @abstractmethod
    def stop(self, *, seconds: Optional[Union[float, int]] = None) -> None:
        ...


NeptuneObjectCallback = Callable[[NeptuneObject], None]
