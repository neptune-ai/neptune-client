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
from datetime import datetime
from typing import Tuple

import pytest

from neptune.internal.utils.iso_dates import (
    _is_valid_iso_date,
    parse_iso_date,
)


def test_parse_iso_dates_if_date_already_is_datetime():
    # Given
    date = datetime(2022, 1, 1, 0, 0, 0, 0)

    # When
    parsed_date = parse_iso_date(date)

    # Then
    assert parsed_date == date


@pytest.mark.parametrize("date", ["2022-01-01", "2022-01-01T00:00:00", "2022-01-01-00:00:00.000Z", "0-01-01-00:00:00Z"])
def test_parse_iso_date_with_incorrect_date_format(date: str):
    # Then
    with pytest.raises(ValueError):
        parse_iso_date(date)


def test_parse_iso_date_with_correct_date_format():
    # Given
    date_strings = ["2024-01-01T00:00:00.000Z", "1000-01-01T13:00:00.000Z", "3000-01-01T00:00:00Z"]
    expected_dates = [
        datetime(2024, 1, 1, 0, 0, 0, 0),
        datetime(1000, 1, 1, 13, 0, 0, 0),
        datetime(3000, 1, 1, 0, 0, 0),
    ]

    for date, expected in zip(date_strings, expected_dates):
        # When
        parsed_date = parse_iso_date(date)

        # Then
        assert parsed_date == expected


@pytest.mark.parametrize(
    "date_test",
    [
        ("2022-01-01", False),  # no time
        ("2022-01-01T00:00Z", False),  # no seconds
        ("2022-01-01T00:00:00", False),  # no "Z" at the end
        ("2022-01-01T00:00:00Z", True),
        ("2022-01-0100:00:00ZT", False),  # "T" in wrong place
        ("2022-01-01-00:00:00Z", False),  # date and time not separated by "T"
        ("2022-01-01T00:00:00.000Z", True),
        ("22-01-01T00:00:00.000000Z", False),  # wrong year format (not 4 digits)
        ("2022-01-01T00:00:00.000000Z", True),
        ("2022-01-01T00:00:00.000000000Z", False),  # too big precision
    ],
)
def test_date_string_validation(date_test: Tuple[str, bool]):
    date, expected = date_test
    assert _is_valid_iso_date(date) == expected
