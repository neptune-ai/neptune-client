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
from datetime import datetime

from mock import (
    Mock,
    call,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.api.models import (
    FloatPointValue,
    FloatSeriesValues,
)


def test__empty():
    # given
    getter_mock = Mock()
    getter_mock.side_effect = [
        FloatSeriesValues(total=0, values=[]),
    ]

    # when
    results = fetch_series_values(
        getter=getter_mock,
        path="some/path",
    )

    # then
    assert list(results) == []
    assert getter_mock.call_count == 1
    assert getter_mock.call_args_list == [
        call(from_step=None, limit=1),
    ]


def test__single_value():
    # given
    now = datetime.now()
    getter_mock = Mock()
    getter_mock.side_effect = [
        FloatSeriesValues(total=1, values=[FloatPointValue(step=1, value=1.0, timestamp=now)]),
    ]

    # when
    results = fetch_series_values(
        getter=getter_mock,
        path="some/path",
    )

    # then
    assert list(results) == [FloatPointValue(step=1, value=1.0, timestamp=now)]
    assert getter_mock.call_count == 1
    assert getter_mock.call_args_list == [
        call(from_step=None, limit=1),
    ]


def test__multiple_values():
    # given
    now = datetime.now()
    getter_mock = Mock()
    getter_mock.side_effect = [
        FloatSeriesValues(
            total=3,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=now),
            ],
        ),
        FloatSeriesValues(
            total=3,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=now),
                FloatPointValue(step=2, value=2.0, timestamp=now.replace(second=2)),
                FloatPointValue(step=3, value=3.0, timestamp=now.replace(second=3)),
            ],
        ),
    ]

    # when
    results = fetch_series_values(
        getter=getter_mock,
        path="some/path",
        step_size=2,
    )

    # then
    assert list(results) == [
        FloatPointValue(step=1, value=1.0, timestamp=now),
        FloatPointValue(step=2, value=2.0, timestamp=now.replace(second=2)),
        FloatPointValue(step=3, value=3.0, timestamp=now.replace(second=3)),
    ]
    assert getter_mock.call_count == 2
    assert getter_mock.call_args_list == [
        call(from_step=None, limit=1),
        call(from_step=0.0, limit=2),
    ]


def test__multiple_pages():
    # given
    now = datetime.now()
    getter_mock = Mock()
    getter_mock.side_effect = [
        FloatSeriesValues(
            total=4,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=now),
            ],
        ),
        FloatSeriesValues(
            total=4,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=now),
                FloatPointValue(step=2, value=2.0, timestamp=now.replace(second=2)),
            ],
        ),
        FloatSeriesValues(
            total=4,
            values=[
                FloatPointValue(step=3, value=3.0, timestamp=now.replace(second=3)),
                FloatPointValue(step=4, value=4.0, timestamp=now.replace(second=4)),
            ],
        ),
    ]

    # when
    results = fetch_series_values(
        getter=getter_mock,
        path="some/path",
        step_size=2,
    )

    # then
    assert list(results) == [
        FloatPointValue(step=1, value=1.0, timestamp=now),
        FloatPointValue(step=2, value=2.0, timestamp=now.replace(second=2)),
        FloatPointValue(step=3, value=3.0, timestamp=now.replace(second=3)),
        FloatPointValue(step=4, value=4.0, timestamp=now.replace(second=4)),
    ]
    assert getter_mock.call_count == 3
    assert getter_mock.call_args_list == [
        call(from_step=None, limit=1),
        call(from_step=0.0, limit=2),
        call(from_step=2.0, limit=2),
    ]
