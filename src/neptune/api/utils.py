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


from typing import (
    Any,
    Callable,
    TypeVar,
)

from neptune.exceptions import NeptuneInvalidQueryException
from neptune.management.internal.api import HTTPBadRequest

__all__ = ["with_leaderboard_entries_search_exception_handler"]

RT = TypeVar("RT")


def with_leaderboard_entries_search_exception_handler(func: Callable[..., RT]) -> Callable[..., RT]:
    def wrapper(*args: Any, **kwargs: Any) -> RT:
        try:
            return func(*args, **kwargs)
        except HTTPBadRequest as e:
            title = e.response.json().get("title")
            if title == "Syntax error":
                query = kwargs["query"]
                raise NeptuneInvalidQueryException(nql_query=query)
            raise e

    return wrapper
