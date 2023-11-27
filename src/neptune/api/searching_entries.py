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
__all__ = ["get_single_page", "iter_over_pages", "to_leaderboard_entry"]

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
)

from bravado.client import construct_request  # type: ignore
from bravado.config import RequestConfig  # type: ignore
from icecream import ic

from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS

if TYPE_CHECKING:
    from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
    from neptune.internal.id_formats import UniqueId
from neptune.internal.backends.nql import NQLQuery, NQLEmptyQuery, NQLQueryAggregate, NQLAggregator, NQLQueryAttribute, \
    NQLAttributeType, NQLAttributeOperator

SUPPORTED_ATTRIBUTE_TYPES = {item.value for item in AttributeType}


def get_single_page(
    *,
    client: "SwaggerClientWrapper",
    project_id: "UniqueId",
    attributes_filter: Dict[str, Any],
    sort_by: str,
    limit: int,
    offset: int,
    types: Optional[Iterable[str]] = None,
    query: Optional["NQLQuery"] = None,
    searching_after: Optional[str] = None,
) -> List[Any]:
    nql_query = query or NQLEmptyQuery()
    if searching_after:
        nql_query = NQLQueryAggregate(
            items=[
                nql_query,
                NQLQueryAttribute(
                    name=sort_by,
                    type=NQLAttributeType.STRING,
                    operator=NQLAttributeOperator.GREATER_THAN,
                    value=searching_after,
                )
            ],
            aggregator=NQLAggregator.AND,
        )

    query_params = {"query": {"query": str(nql_query)}}
    params = {
        "projectIdentifier": project_id,
        "type": types,
        "params": {
            "sorting": {
                "dir": "ascending",
                "aggregationMode": "none",
                "sortBy": {"name": sort_by, "type": "string"},
            },
            **query_params,
            **attributes_filter,
            "pagination": {"limit": limit, "offset": offset},
        },
    }

    # ic(params)

    request_options = DEFAULT_REQUEST_KWARGS.get("_request_options", {})
    request_config = RequestConfig(request_options, True)
    request_params = construct_request(client.api.searchLeaderboardEntries, request_options, **params)

    http_client = client.swagger_spec.http_client

    result = (
        http_client.request(request_params, operation=None, request_config=request_config)
        .response()
        .incoming_response.json()
    )

    return list(map(to_leaderboard_entry, result.get("entries", [])))


def to_leaderboard_entry(entry: Dict[str, Any]) -> LeaderboardEntry:
    return LeaderboardEntry(
        id=entry["experimentId"],
        attributes=[
            AttributeWithProperties(
                path=attr["name"],
                type=AttributeType(attr["type"]),
                properties=attr.__getitem__(f"{attr['type']}Properties"),
            )
            for attr in entry["attributes"]
            if attr["type"] in SUPPORTED_ATTRIBUTE_TYPES
        ],
    )


def find_attribute(*, entry: LeaderboardEntry, path: str) -> Optional[AttributeWithProperties]:
    for attr in entry.attributes:
        if attr.path == path:
            return attr
    return None


def iter_over_pages(
    *, iter_once: Callable[..., List[Any]], step: int, sort_by: str = 'sys/id', max_server_offset: int = 10_000
) -> Generator[Any, None, None]:
    step = 10
    batch = None
    searching_after = None
    next_searching_after = None

    while searching_after is not None or batch is None:
        for offset in range(0, max_server_offset, step):
            batch = iter_once(limit=min(step, max_server_offset-offset), offset=offset, sort_by=sort_by, searching_after=searching_after)
            if not batch:
                return

            yield from batch

            sort_by_attribute = find_attribute(entry=batch[-1], path=sort_by)
            next_searching_after = sort_by_attribute.properties['value'] if sort_by_attribute else None

        searching_after = next_searching_after
