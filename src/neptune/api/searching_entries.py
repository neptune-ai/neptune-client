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
    Type,
    Union,
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
from neptune.internal.backends.utils import construct_progress_bar
from neptune.internal.init.parameters import MAX_SERVER_OFFSET
from neptune.typing import ProgressBarCallback

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
    sort_by_column_type: Optional[str] = None,
    ascending: bool = False,
    types: Optional[Iterable[str]] = None,
    query: Optional["NQLQuery"] = None,
    searching_after: Optional[str] = None,
) -> Any:
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
                "dir": "ascending" if ascending else "descending",
                "aggregationMode": "none",
                "sortBy": {
                    "name": sort_by,
                    "type": sort_by_column_type if sort_by_column_type else AttributeType.STRING.value,
                },
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

    return (
        http_client.request(request_params, operation=None, request_config=request_config)
        .response()
        .incoming_response.json()
    )


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
    sort_by_column_type: Optional[str] = None,
    ascending: bool = False,
    progress_bar: Optional[Union[bool, Type[ProgressBarCallback]]] = None,
    **kwargs: Any,
) -> Generator[Any, None, None]:
    searching_after = None
    last_page = None

    total = 0

    progress_bar = progress_bar if step_size >= total else None

    with construct_progress_bar(progress_bar, "Fetching table...") as bar:
        # beginning of the first page
        bar.update(
            by=0,
            total=get_single_page(
                limit=0,
                offset=0,
                **kwargs,
            ).get("matchingItemCount", 0),
        )

        while True:
            if last_page:
                page_attribute = find_attribute(entry=last_page[-1], path=sort_by)

                if not page_attribute:
                    raise ValueError(f"Cannot find attribute {sort_by} in last page")

                searching_after = page_attribute.properties["value"]

            for offset in range(0, max_offset, step_size):
                result = get_single_page(
                    limit=min(step_size, max_offset - offset),
                    offset=offset,
                    sort_by=sort_by,
                    sort_by_column_type=sort_by_column_type,
                    searching_after=searching_after,
                    ascending=ascending,
                    **kwargs,
                )

                # fetch the item count everytime a new page is started
                if offset == 0:
                    total += result.get("matchingItemCount", 0)

                page = _entries_from_page(result)

                if not page:
                    return

                bar.update(by=step_size, total=total)

                yield from page

                last_page = page


def _entries_from_page(single_page: Dict[str, Any]) -> List[LeaderboardEntry]:
    return list(map(to_leaderboard_entry, single_page.get("entries", [])))
