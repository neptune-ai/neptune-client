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
from neptune.api.searching_entries import (
    iter_over_pages,
    to_leaderboard_entry,
)
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
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


def test__iter_over_pages__general():
    # given
    def iter_once(limit: int, offset: int):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        return values[offset : offset + limit]

    # when
    result = list(iter_over_pages(iter_once=iter_once, step=3))

    # then
    assert result == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test__iter_over_pages__empty():
    # given
    def iter_once(limit: int, offset: int):
        return []

    # when
    result = list(iter_over_pages(iter_once=iter_once, step=3))

    # then
    assert result == []


def test__iter_over_pages__max_server_offset():
    # given
    def iter_once(limit: int, offset: int):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        return values[offset : offset + limit]

    # when
    result = list(iter_over_pages(iter_once=iter_once, step=3, max_server_offset=5))

    # then
    assert result == [1, 2, 3, 4, 5]
