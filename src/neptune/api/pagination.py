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
import itertools
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Iterator,
    List,
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
    extract_entries: Callable[[T], List[Entry]],
    page_size: int = 50,
    limit: Optional[int] = None,
    **kwargs: Any,
) -> Iterator[Entry]:
    """
    Generic approach to pagination via `NextPage`
    """
    counter = 0
    data = getter(**kwargs, next_page=NextPage(limit=page_size, next_page_token=None))
    results = extract_entries(data)
    if limit is not None:
        counter = len(results[:limit])

    yield from itertools.islice(results, limit)

    while data.next_page is not None and data.next_page.next_page_token is not None:
        to_fetch = page_size
        if limit is not None:
            if counter >= limit:
                break
            to_fetch = min(page_size, limit - counter)

        data = getter(**kwargs, next_page=NextPage(limit=to_fetch, next_page_token=data.next_page.next_page_token))
        results = extract_entries(data)
        if limit is not None:
            counter += len(results[:to_fetch])

        yield from itertools.islice(results, to_fetch)
