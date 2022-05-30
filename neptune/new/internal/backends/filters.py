from typing import Callable, Iterable

from neptune.new.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
)


def state_attribute(state: str) -> NQLQueryAttribute:
    return NQLQueryAttribute(
        name="sys/state",
        type=NQLAttributeType.EXPERIMENT_STATE,
        operator=NQLAttributeOperator.EQUALS,
        value=state,
    )


def _list_filter(
    values: Iterable[str],
    attribute_builder: Callable,
    aggregator: NQLAggregator = NQLAggregator.OR,
) -> NQLQuery:
    return NQLQueryAggregate(items=list(map(attribute_builder, values)), aggregator=aggregator)


def states_filter(states: Iterable[str]) -> NQLQuery:
    return _list_filter(states, state_attribute)


def filter_by_owners(owners: Iterable[str]) -> NQLQuery:
    return NQLQueryAggregate(
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


def ids_filter(ids: Iterable[str]) -> NQLQuery:
    return NQLQueryAggregate(
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


def tags_filter(tags: Iterable[str]) -> NQLQuery:
    return NQLQueryAggregate(
        items=[
            NQLQueryAttribute(
                name="sys/tags",
                type=NQLAttributeType.STRING_SET,
                operator=NQLAttributeOperator.CONTAINS,
                value=tag,
            )
            for tag in tags
        ],
        aggregator=NQLAggregator.OR,
    )
