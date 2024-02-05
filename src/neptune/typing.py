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
__all__ = ["SupportsNamespaces", "NeptuneObject", "NeptuneObjectCallback", "ProgressBarCallback", "ProgressBarType"]

import abc
import contextlib
from typing import (
    Any,
    Optional,
    Type,
    Union,
)

from typing_extensions import TypeAlias

from neptune.metadata_containers.abstract import (
    NeptuneObject,
    NeptuneObjectCallback,
    SupportsNamespaces,
)


class ProgressBarCallback(contextlib.AbstractContextManager):
    """Abstract base class for progress bar callbacks.

    You can use this class to implement your own progress bar callback that will be invoked in table fetching methods:

    - `fetch_runs_table()`
    - `fetch_models_table()`
    - `fetch_model_versions_table()`

    Example using `click`:
        >>> from typing import Any, Optional, Type
        >>> from types import TracebackType
        >>> from neptune.typing import ProgressBarCallback
        >>> class ClickProgressBar(ProgressBarCallback):
        ...     def __init__(self, *, description: Optional[str] = None, **_: Any) -> None:
        ...         super().__init__()
        ...         from click import progressbar
        ...
        ...         self._progress_bar = progressbar(iterable=None, length=1, label=description)
        ...
        ...     def update(self, *, by: int, total: Optional[int] = None) -> None:
        ...         if total:
        ...             self._progress_bar.length = total
        ...         self._progress_bar.update(by)
        ...
        ...     def __enter__(self) -> "ClickProgressBar":
        ...         self._progress_bar.__enter__()
        ...         return self
        ...
        ...     def __exit__(
        ...         self,
        ...         exc_type: Optional[Type[BaseException]],
        ...         exc_val: Optional[BaseException],
        ...         exc_tb: Optional[TracebackType],
        ...     ) -> None:
        ...         self._progress_bar.__exit__(exc_type, exc_val, exc_tb)
        >>> from neptune import init_project
        >>> with init_project() as project:
        ...     project.fetch_runs_table(progress_bar=ClickProgressBar)
        ...     project.fetch_models_table(progress_bar=ClickProgressBar)

        IMPORTANT: Pass a type, not an instance to the `progress_bar` argument.
        That is, `ClickProgressBar`, not `ClickProgressBar()`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def update(self, *, by: int, total: Optional[int] = None) -> None:
        ...


ProgressBarType: TypeAlias = Union[bool, Type[ProgressBarCallback]]
