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
from typing import (
    Any,
    List,
)

from mock import (
    Mock,
    call,
)

from neptune.api.models import NextPage
from neptune.api.pagination import paginate_over


def extract_entries_empty(data: Any) -> List[int]:
    return []


def extract_entries(data: Any) -> List[int]:
    return [1, 2, 3]


def test__empty():
    # given
    getter = Mock(side_effect=[Mock(next_page=None)])

    # when
    entries = list(paginate_over(getter=getter, extract_entries=extract_entries_empty))

    # then
    assert entries == []


def test__single_page():
    # given
    getter = Mock(side_effect=[Mock(next_page=None)])

    # when
    entries = list(paginate_over(getter=getter, extract_entries=extract_entries))

    # then
    assert entries == [1, 2, 3]


def test__multiple_pages():
    # given
    getter = Mock(
        side_effect=[
            Mock(next_page=NextPage(next_page_token="aa", limit=None)),
            Mock(next_page=NextPage(next_page_token="bb", limit=None)),
            Mock(next_page=None),
        ]
    )

    # when
    entries = list(paginate_over(getter=getter, extract_entries=extract_entries))

    # then
    assert entries == [1, 2, 3, 1, 2, 3, 1, 2, 3]

    assert getter.call_count == 3
    assert getter.call_args_list == [
        call(next_page=None),
        call(next_page=NextPage(next_page_token="aa", limit=None)),
        call(next_page=NextPage(next_page_token="bb", limit=None)),
    ]


def test__kwargs_passed():
    # given
    getter = Mock(
        side_effect=[
            Mock(next_page=NextPage(next_page_token="aa", limit=None)),
            Mock(next_page=NextPage(next_page_token="bb", limit=None)),
            Mock(next_page=None),
        ]
    )

    # when
    entries = list(paginate_over(getter=getter, extract_entries=extract_entries, a=1, b=2))

    # then
    assert entries == [1, 2, 3, 1, 2, 3, 1, 2, 3]

    assert getter.call_count == 3
    assert getter.call_args_list == [
        call(a=1, b=2, next_page=None),
        call(a=1, b=2, next_page=NextPage(next_page_token="aa", limit=None)),
        call(a=1, b=2, next_page=NextPage(next_page_token="bb", limit=None)),
    ]
