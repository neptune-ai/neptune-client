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
__all__ = ["get_single_page", "iter_over_pages"]

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
)

from bravado.client import construct_request  # type: ignore
from bravado.config import RequestConfig  # type: ignore

from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLEmptyQuery,
    NQLQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
)
from neptune.internal.init.parameters import MAX_SERVER_OFFSET

if TYPE_CHECKING:
    from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
    from neptune.internal.id_formats import UniqueId


SUPPORTED_ATTRIBUTE_TYPES = {item.value for item in AttributeType}


def get_single_page(
    *,
    client: "SwaggerClientWrapper",
    project_id: "UniqueId",
    attributes_filter: Dict[str, Any],
    limit: int,
    offset: int,
    sort_by: Optional[str] = None,
    types: Optional[Iterable[str]] = None,
    query: Optional["NQLQuery"] = None,
    searching_after: Optional[str] = None,
) -> List[Any]:
    normalized_query = query or NQLEmptyQuery()
    if sort_by and searching_after:
        sort_by_as_nql = NQLQueryAttribute(
            name=sort_by,
            type=NQLAttributeType.STRING,
            operator=NQLAttributeOperator.GREATER_THAN,
            value=searching_after,
        )

        if not isinstance(normalized_query, NQLEmptyQuery):
            normalized_query = NQLQueryAggregate(items=[normalized_query, sort_by_as_nql], aggregator=NQLAggregator.AND)
        else:
            normalized_query = sort_by_as_nql

    sorting = (
        {
            "sorting": {
                "dir": "ascending",
                "aggregationMode": "none",
                "sortBy": {"name": sort_by, "type": "string"},
            }
        }
        if sort_by
        else {}
    )

    params = {
        "projectIdentifier": project_id,
        "type": types,
        "params": {
            **sorting,
            **attributes_filter,
            "query": {"query": str(normalized_query)},
            "pagination": {"limit": limit, "offset": offset},
        },
    }

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
    return next((attr for attr in entry.attributes if attr.path == path), None)


def iter_over_pages(
    *,
    step_size: int,
    sort_by: str = "sys/id",
    max_offset: int = MAX_SERVER_OFFSET,
    **kwargs: Any,
) -> Generator[Any, None, None]:
    searching_after = None
    last_page = None

    while True:
        if last_page:
            page_attribute = find_attribute(entry=last_page[-1], path=sort_by)

            if not page_attribute:
                raise ValueError(f"Cannot find attribute {sort_by} in last page")

            searching_after = page_attribute.properties["value"]

        for offset in range(0, max_offset, step_size):
            page = get_single_page(
                limit=min(step_size, max_offset - offset),
                offset=offset,
                sort_by=sort_by,
                searching_after=searching_after,
                **kwargs,
            )

            if not page:
                return

            yield from page

            last_page = page
