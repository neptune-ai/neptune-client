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

__all__ = [
    "DATE_FORMAT",
    "parse_dates",
    "prepare_nql_query",
]

from datetime import datetime
from typing import (
    Any,
    Generator,
    Iterable,
    List,
    Optional,
    Union,
)

from neptune.common.warnings import (
    NeptuneDeprecationWarning,
    NeptuneWarning,
    warn_once,
)
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
    RawNQLQuery,
)
from neptune.internal.utils.run_state import RunState

DATE_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"


def prepare_nql_query(
    ids: Optional[Iterable[str]],
    states: Optional[Iterable[str]],
    owners: Optional[Iterable[str]],
    tags: Optional[Iterable[str]],
    trashed: Optional[bool],
) -> NQLQueryAggregate:
    query_items: List[Union[NQLQueryAttribute, NQLQueryAggregate]] = []

    if trashed is not None:
        query_items.append(
            NQLQueryAttribute(
                name="sys/trashed",
                type=NQLAttributeType.BOOLEAN,
                operator=NQLAttributeOperator.EQUALS,
                value=trashed,
            )
        )

    if ids:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/id",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.EQUALS,
                        value=api_id,
                    )
                    for api_id in ids
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if states:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/state",
                        type=NQLAttributeType.EXPERIMENT_STATE,
                        operator=NQLAttributeOperator.EQUALS,
                        value=RunState.from_string(state).to_api(),
                    )
                    for state in states
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if owners:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/owner",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.EQUALS,
                        value=owner,
                    )
                    for owner in owners
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if tags:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/tags",
                        type=NQLAttributeType.STRING_SET,
                        operator=NQLAttributeOperator.CONTAINS,
                        value=tag,
                    )
                    for tag in tags
                ],
                aggregator=NQLAggregator.AND,
            )
        )

    query = NQLQueryAggregate(items=query_items, aggregator=NQLAggregator.AND)
    return query


def parse_dates(leaderboard_entries: Iterable[LeaderboardEntry]) -> Generator[LeaderboardEntry, None, None]:
    yield from [_parse_entry(entry) for entry in leaderboard_entries]


def _parse_entry(entry: LeaderboardEntry) -> LeaderboardEntry:
    try:
        return LeaderboardEntry(
            entry.id,
            attributes=[
                AttributeWithProperties(
                    attribute.path,
                    attribute.type,
                    {
                        **attribute.properties,
                        "value": datetime.strptime(attribute.properties["value"], DATE_FORMAT),
                    },
                )
                if attribute.type == AttributeType.DATETIME
                else attribute
                for attribute in entry.attributes
            ],
        )
    except TypeError:
        # date is already in the right format
        return entry
    except ValueError:
        # the parsing format is incorrect
        warn_once(
            "Date parsing failed. The date format is incorrect. Returning as string instead of datetime.",
            exception=NeptuneWarning,
        )
        return entry


def deprecated_func_arg_warning_check(fname: str, vname: str, var: Any) -> None:
    if var is not None:
        msg = f"""The argument '{vname}' of the function '{fname}' is deprecated and will be removed in the future."""
        warn_once(msg, exception=NeptuneDeprecationWarning)


def build_raw_query(query: str, trashed: Optional[bool]) -> NQLQuery:
    raw_nql = RawNQLQuery(query)

    if trashed is None:
        return raw_nql

    nql = NQLQueryAggregate(
        items=[
            raw_nql,
            NQLQueryAttribute(
                name="sys/trashed", type=NQLAttributeType.BOOLEAN, operator=NQLAttributeOperator.EQUALS, value=trashed
            ),
        ],
        aggregator=NQLAggregator.AND,
    )
    return nql
