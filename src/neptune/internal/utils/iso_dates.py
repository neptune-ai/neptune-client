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

__all__ = ["parse_iso_date"]

import datetime
from typing import Union

DATE_FORMAT_LONG: str = "%Y-%m-%dT%H:%M:%S.%fZ"
DATE_FORMAT_SHORT: str = "%Y-%m-%dT%H:%M:%SZ"


def parse_iso_date(date: Union[str, datetime.datetime]) -> datetime.datetime:
    if isinstance(date, datetime.datetime):
        return date

    date_format = DATE_FORMAT_LONG if _is_long_date_format(date) else DATE_FORMAT_SHORT

    return datetime.datetime.strptime(date, date_format)


def _is_long_date_format(date_string: str) -> bool:
    return len(date_string.split(".")) == 2
