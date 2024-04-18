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
__all__ = ("paginate_over",)

import abc
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
)

from typing_extensions import Protocol

from neptune.api.models import NextPage


@dataclass
class WithPagination(abc.ABC):
    next_page: Optional[NextPage]


T = TypeVar("T", bound=WithPagination)
Entry = TypeVar("Entry")


class Paginatable(Protocol):
    def __call__(self, *, next_page: Optional[NextPage] = None, **kwargs: Any) -> Any: ...


def paginate_over(
    getter: Paginatable,
    extract_entries: Callable[[T], Iterable[Entry]],
    **kwargs: Any,
) -> Iterator[Entry]:
    """
    Generic approach to pagination via `NextPage`
    """
    data = getter(**kwargs, next_page=None)
    yield from extract_entries(data)

    while data.next_page is not None and data.next_page.next_page_token is not None:
        data = getter(**kwargs, next_page=data.next_page)
        yield from extract_entries(data)
