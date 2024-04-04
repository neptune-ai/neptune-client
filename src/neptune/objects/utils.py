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
    "prepare_nql_query",
]

from typing import (
    Iterable,
    List,
    Optional,
    Union,
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
