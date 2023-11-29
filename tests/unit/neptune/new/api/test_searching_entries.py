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
from mock import (
    call,
    patch,
)

from neptune.api.searching_entries import (
    iter_over_pages,
    to_leaderboard_entry,
)
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)


def test__to_leaderboard_entry():
    # given
    entry = {
        "experimentId": "foo",
        "attributes": [
            {
                "name": "plugh",
                "type": "float",
                "floatProperties": {
                    "value": 1.0,
                },
            },
            {
                "name": "sys/id",
                "type": "string",
                "stringProperties": {
                    "value": "TEST-123",
                },
            },
        ],
    }

    # when
    result = to_leaderboard_entry(entry=entry)

    # then
    assert result.id == "foo"
    assert result.attributes == [
        AttributeWithProperties(
            path="plugh",
            type=AttributeType.FLOAT,
            properties={
                "value": 1.0,
            },
        ),
        AttributeWithProperties(
            path="sys/id",
            type=AttributeType.STRING,
            properties={
                "value": "TEST-123",
            },
        ),
    ]


@patch("neptune.api.searching_entries.get_single_page")
def test__iter_over_pages__single_pagination(get_single_page):
    # given
    get_single_page.side_effect = [
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})
                ],
            ),
        ],
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "f"})
                ],
            ),
        ],
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "g"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "h"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "j"})
                ],
            ),
        ],
        None,
    ]

    # when
    result = list(iter_over_pages(step_size=3))

    # then
    assert result == [
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "f"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "g"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "h"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "j"})],
        ),
    ]
    assert get_single_page.mock_calls == [
        call(limit=3, offset=0, sort_by="sys/id", searching_after=None),
        call(limit=3, offset=3, sort_by="sys/id", searching_after=None),
        call(limit=3, offset=6, sort_by="sys/id", searching_after=None),
        call(limit=3, offset=9, sort_by="sys/id", searching_after=None),
    ]


@patch("neptune.api.searching_entries.get_single_page")
def test__iter_over_pages__multiple_search_after(get_single_page):
    # given
    get_single_page.side_effect = [
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})
                ],
            ),
        ],
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "f"})
                ],
            ),
        ],
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "g"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "h"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "j"})
                ],
            ),
        ],
        None,
    ]

    # when
    result = list(iter_over_pages(step_size=3, max_offset=6))

    # then
    assert result == [
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "f"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "g"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "h"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "j"})],
        ),
    ]
    assert get_single_page.mock_calls == [
        call(limit=3, offset=0, sort_by="sys/id", searching_after=None),
        call(limit=3, offset=3, sort_by="sys/id", searching_after=None),
        call(limit=3, offset=0, sort_by="sys/id", searching_after="f"),
        call(limit=3, offset=3, sort_by="sys/id", searching_after="f"),
    ]


@patch("neptune.api.searching_entries.get_single_page")
def test__iter_over_pages__empty(get_single_page):
    # given
    get_single_page.side_effect = [[]]

    # when
    result = list(iter_over_pages(step_size=3))

    # then
    assert result == []
    assert get_single_page.mock_calls == [call(limit=3, offset=0, sort_by="sys/id", searching_after=None)]


@patch("neptune.api.searching_entries.get_single_page")
def test__iter_over_pages__max_server_offset(get_single_page):
    # given
    get_single_page.side_effect = [
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})
                ],
            ),
        ],
        [
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})
                ],
            ),
            LeaderboardEntry(
                id="foo",
                attributes=[
                    AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})
                ],
            ),
        ],
        None,
    ]

    # when
    result = list(iter_over_pages(step_size=3, max_offset=5))

    # then
    assert result == [
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "a"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "b"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "c"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "d"})],
        ),
        LeaderboardEntry(
            id="foo",
            attributes=[AttributeWithProperties(path="sys/id", type=AttributeType.STRING, properties={"value": "e"})],
        ),
    ]
    assert get_single_page.mock_calls == [
        call(offset=0, limit=3, sort_by="sys/id", searching_after=None),
        call(offset=3, limit=2, sort_by="sys/id", searching_after=None),
        call(offset=0, limit=3, sort_by="sys/id", searching_after="e"),
    ]
