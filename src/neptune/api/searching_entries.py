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
from bravado.exception import HTTPBadRequest  # type: ignore
from typing_extensions import (
    Literal,
    TypeAlias,
)

from neptune.exceptions import NeptuneInvalidQueryException
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
from neptune.typing import ProgressBarType

if TYPE_CHECKING:
    from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
    from neptune.internal.id_formats import UniqueId


SUPPORTED_ATTRIBUTE_TYPES = {item.value for item in AttributeType}

SORT_BY_COLUMN_TYPE: TypeAlias = Literal["string", "datetime", "integer", "boolean", "float"]


class NoLimit(int):
    def __gt__(self, other: Any) -> bool:
        return True

    def __lt__(self, other: Any) -> bool:
        return False

    def __ge__(self, other: Any) -> bool:
        return True

    def __le__(self, other: Any) -> bool:
        return False

    def __eq__(self, other: Any) -> bool:
        return False

    def __ne__(self, other: Any) -> bool:
        return True


def get_single_page(
    *,
    client: "SwaggerClientWrapper",
    project_id: "UniqueId",
    attributes_filter: Dict[str, Any],
    limit: int,
    offset: int,
    sort_by: str,
    sort_by_column_type: SORT_BY_COLUMN_TYPE,
    ascending: bool,
    types: Optional[Iterable[str]],
    query: Optional["NQLQuery"],
    searching_after: Optional[str],
) -> Any:
    normalized_query = query or NQLEmptyQuery()
    sort_by_column_type = sort_by_column_type if sort_by_column_type else AttributeType.STRING.value
    if sort_by and searching_after:
        sort_by_as_nql = NQLQueryAttribute(
            name=sort_by,
            type=NQLAttributeType(sort_by_column_type),
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

    try:
        return (
            http_client.request(request_params, operation=None, request_config=request_config)
            .response()
            .incoming_response.json()
        )
    except HTTPBadRequest as e:
        title = e.response.json().get("title")
        if title == "Syntax error":
            raise NeptuneInvalidQueryException(nql_query=str(normalized_query))
        raise e


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
    limit: Optional[int],
    sort_by: str,
    sort_by_column_type: SORT_BY_COLUMN_TYPE,
    ascending: bool,
    progress_bar: Optional[ProgressBarType],
    max_offset: int = MAX_SERVER_OFFSET,
    **kwargs: Any,
) -> Generator[Any, None, None]:
    searching_after = None
    last_page = None

    total = get_single_page(
        limit=0,
        offset=0,
        sort_by=sort_by,
        ascending=ascending,
        sort_by_column_type=sort_by_column_type,
        searching_after=None,
        **kwargs,
    ).get("matchingItemCount", 0)

    limit = limit if limit is not None else NoLimit()

    total = total if total < limit else limit

    progress_bar = False if total <= step_size else progress_bar  # disable progress bar if only one page is fetched

    extracted_records = 0

    with construct_progress_bar(progress_bar, "Fetching table...") as bar:
        # beginning of the first page
        bar.update(
            by=0,
            total=total,
        )

        while True:
            if last_page:
                page_attribute = find_attribute(entry=last_page[-1], path=sort_by)

                if not page_attribute:
                    raise ValueError(f"Cannot find attribute {sort_by} in last page")

                searching_after = page_attribute.properties["value"]

            for offset in range(0, max_offset, step_size):
                local_limit = min(step_size, max_offset - offset)
                if extracted_records + local_limit > limit:
                    local_limit = limit - extracted_records
                result = get_single_page(
                    limit=local_limit,
                    offset=offset,
                    sort_by=sort_by,
                    sort_by_column_type=sort_by_column_type,
                    searching_after=searching_after,
                    ascending=ascending,
                    **kwargs,
                )

                # fetch the item count everytime a new page is started (except for the very fist page)
                if offset == 0 and last_page is not None:
                    total += result.get("matchingItemCount", 0)

                total = min(total, limit)

                page = _entries_from_page(result)
                extracted_records += len(page)
                bar.update(by=len(page), total=total)

                if not page:
                    return

                yield from page

                if extracted_records == limit:
                    return

                last_page = page


def _entries_from_page(single_page: Dict[str, Any]) -> List[LeaderboardEntry]:
    return list(map(to_leaderboard_entry, single_page.get("entries", [])))
