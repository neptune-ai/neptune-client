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
    List,
    Sequence,
)

import pytest
from bravado.exception import HTTPBadRequest
from mock import (
    Mock,
    call,
    patch,
)

from neptune.api.searching_entries import (
    get_single_page,
    iter_over_pages,
)
from neptune.exceptions import NeptuneInvalidQueryException
from neptune.api.models import Field, StringField, FieldType, LeaderboardEntry


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
    assert result.object_id == "foo"
    assert result.fields == [
        Field(
            path="plugh",
            type=FieldType.FLOAT,
            properties={
                "value": 1.0,
            },
        ),
        Field(
            path="sys/id",
            type=FieldType.STRING,
            properties={
                "value": "TEST-123",
            },
        ),
    ]


@patch("neptune.api.searching_entries._entries_from_page")
@patch("neptune.api.searching_entries.get_single_page", return_value={"matchingItemCount": 9})
def test__iter_over_pages__single_pagination(get_single_page, entries_from_page):
    # given
    entries_from_page.side_effect = [
        generate_leaderboard_entries(values=["a", "b", "c"]),
        generate_leaderboard_entries(values=["d", "e", "f"]),
        generate_leaderboard_entries(values=["g", "h", "j"]),
        [],
    ]

    # when
    result = list(
        iter_over_pages(
            step_size=3,
            limit=None,
            sort_by="sys/id",
            sort_by_column_type=None,
            ascending=False,
            progress_bar=None,
        )
    )

    # then
    assert result == generate_leaderboard_entries(values=["a", "b", "c", "d", "e", "f", "g", "h", "j"])
    assert get_single_page.mock_calls == [
        # total checking
        call(limit=0, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=3, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=6, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=9, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
    ]


@patch("neptune.api.searching_entries._entries_from_page")
@patch("neptune.api.searching_entries.get_single_page", return_value={"matchingItemCount": 9})
def test__iter_over_pages__multiple_search_after(get_single_page, entries_from_page):
    # given
    entries_from_page.side_effect = [
        generate_leaderboard_entries(values=["a", "b", "c"]),
        generate_leaderboard_entries(values=["d", "e", "f"]),
        generate_leaderboard_entries(values=["g", "h", "j"]),
        [],
    ]

    # when
    result = list(
        iter_over_pages(
            step_size=3,
            limit=None,
            sort_by="sys/id",
            sort_by_column_type=None,
            ascending=False,
            progress_bar=None,
            max_offset=6,
        )
    )

    # then
    assert result == generate_leaderboard_entries(values=["a", "b", "c", "d", "e", "f", "g", "h", "j"])
    assert get_single_page.mock_calls == [
        # total checking
        call(limit=0, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=3, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after="f"),
        call(limit=3, offset=3, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after="f"),
    ]


@patch("neptune.api.searching_entries._entries_from_page")
@patch("neptune.api.searching_entries.get_single_page", return_value={"matchingItemCount": 1})
def test__iter_over_pages__empty(get_single_page, entries_from_page):
    # given
    entries_from_page.side_effect = [[]]

    # when
    result = list(
        iter_over_pages(
            step_size=3,
            limit=None,
            sort_by="sys/id",
            sort_by_column_type=None,
            ascending=False,
            progress_bar=None,
        )
    )

    # then
    assert result == []
    assert get_single_page.mock_calls == [
        # total checking
        call(limit=0, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(limit=3, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
    ]


@patch("neptune.api.searching_entries._entries_from_page")
@patch("neptune.api.searching_entries.get_single_page", return_value={"matchingItemCount": 1})
def test__iter_over_pages__max_server_offset(get_single_page, entries_from_page):
    # given
    entries_from_page.side_effect = [
        generate_leaderboard_entries(values=["a", "b", "c"]),
        generate_leaderboard_entries(values=["d", "e"]),
        [],
    ]

    # when
    result = list(
        iter_over_pages(
            step_size=3,
            limit=None,
            sort_by="sys/id",
            sort_by_column_type=None,
            ascending=False,
            progress_bar=None,
            max_offset=5,
        )
    )

    # then
    assert result == generate_leaderboard_entries(values=["a", "b", "c", "d", "e"])
    assert get_single_page.mock_calls == [
        # total checking
        call(limit=0, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(offset=0, limit=3, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(offset=3, limit=2, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(offset=0, limit=3, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after="e"),
    ]


@patch("neptune.api.searching_entries._entries_from_page")
@patch("neptune.api.searching_entries.get_single_page", return_value={"matchingItemCount": 5})
def test__iter_over_pages__limit(get_single_page, entries_from_page):
    # since the limiting itself takes place in an external service, we can't test the results
    # we can only test if the limit is properly passed to the external service call

    # given
    entries_from_page.side_effect = [
        generate_leaderboard_entries(values=["a", "b"]),
        generate_leaderboard_entries(values=["c", "d"]),
        generate_leaderboard_entries(values=["e"]),
        [],
    ]

    # when
    list(
        iter_over_pages(
            step_size=2,
            limit=4,
            sort_by="sys/id",
            sort_by_column_type=None,
            ascending=False,
            progress_bar=None,
        )
    )

    # then
    assert get_single_page.mock_calls == [
        # total checking
        call(limit=0, offset=0, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(offset=0, limit=2, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
        call(offset=2, limit=2, sort_by="sys/id", ascending=False, sort_by_column_type=None, searching_after=None),
    ]


def generate_leaderboard_entries(values: Sequence, experiment_id: str = "foo") -> List[LeaderboardEntry]:
    return [
        LeaderboardEntry(
            object_id=experiment_id,
            fields=[StringField(path="sys/id", value=value)],
        )
        for value in values
    ]


@patch("neptune.api.searching_entries.construct_request")
def test_get_single_page_error_handling(construct_request_mock):
    # given
    bravado_exception = HTTPBadRequest(response=Mock())
    bravado_exception.response.json.return_value = {"title": "Syntax error"}

    failing_clinet = Mock()
    failing_clinet.swagger_spec.http_client.request.side_effect = bravado_exception

    # then
    with pytest.raises(NeptuneInvalidQueryException):
        get_single_page(
            project_id="id",
            attributes_filter={},
            types=None,
            query="invalid_query",
            limit=0,
            offset=0,
            sort_by="sys/id",
            ascending=False,
            sort_by_column_type=None,
            searching_after=None,
            client=failing_clinet,
        )
