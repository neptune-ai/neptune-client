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

__all__ = [
    "ProgressBarCallback",
    "TqdmNotebookProgressBar",
    "TqdmProgressBar",
    "ClickProgressBar",
    "IPythonProgressBar",
    "NullProgressBar",
]

import abc
import contextlib
from types import TracebackType
from typing import (
    Any,
    Callable,
    Optional,
    Type,
)

GENERIC_FUNC_TYPE = Callable[[Any], Any]


def _handle_import_error(dependency: str) -> Callable[[GENERIC_FUNC_TYPE], GENERIC_FUNC_TYPE]:
    def deco(func: GENERIC_FUNC_TYPE) -> GENERIC_FUNC_TYPE:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                raise ModuleNotFoundError(
                    f"Required dependency for progress bar not found. Run 'pip install {dependency}'."
                ) from e

        return wrapper

    return deco


class ProgressBarCallback(contextlib.AbstractContextManager):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def update(self, *, by: int, total: Optional[int] = None) -> None:
        ...


class NullProgressBar(ProgressBarCallback):
    def __init__(self, *args: Any, description: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        pass


class TqdmProgressBar(ProgressBarCallback):
    @_handle_import_error(dependency="tqdm")
    def __init__(
        self,
        *args: Any,
        description: Optional[str] = None,
        unit: Optional[str] = None,
        unit_scale: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        from tqdm import tqdm

        unit = unit if unit else ""

        self._progress_bar = tqdm(desc=description, unit=unit, unit_scale=unit_scale, **kwargs)

    def __enter__(self) -> "TqdmProgressBar":
        self._progress_bar.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._progress_bar.__exit__(exc_type, exc_val, exc_tb)

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        if total:
            self._progress_bar.total = total
        self._progress_bar.update(by)


class ClickProgressBar(ProgressBarCallback):
    @_handle_import_error(dependency="click")
    def __init__(self, *args: Any, description: Optional[str] = None, **_: Any) -> None:
        super().__init__(*args, **_)
        from click import progressbar

        self._progress_bar = progressbar(iterable=None, length=1, label=description)

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        if total:
            self._progress_bar.length = total
        self._progress_bar.update(by)

    def __enter__(self) -> "ClickProgressBar":
        self._progress_bar.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._progress_bar.__exit__(exc_type, exc_val, exc_tb)


class TqdmNotebookProgressBar(ProgressBarCallback):
    @_handle_import_error(dependency="tqdm")
    def __init__(
        self,
        *args: Any,
        description: Optional[str] = None,
        unit: Optional[str] = None,
        unit_scale: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        from tqdm.notebook import tqdm

        unit = unit if unit else ""

        self._progress_bar = tqdm(desc=description, unit=unit, unit_scale=unit_scale, **kwargs)

    def __enter__(self) -> "TqdmNotebookProgressBar":
        self._progress_bar.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._progress_bar.__exit__(exc_type, exc_val, exc_tb)

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        if total:
            self._progress_bar.total = total
        self._progress_bar.update(by)


class IPythonProgressBar(ProgressBarCallback):
    def __init__(self, *args: Any, description: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        from IPython.display import display  # type: ignore[import]
        from ipywidgets import IntProgress  # type: ignore[import]

        self._progress_bar = IntProgress(description=description, **kwargs)
        display(self._progress_bar)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        if total is not None:
            self._progress_bar.max = total
        self._progress_bar.value += by
