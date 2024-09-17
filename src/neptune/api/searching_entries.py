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
__all__ = ["get_single_page", "iter_over_pages", "find_attribute"]

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    Optional,
)

from bravado.exception import HTTPBadRequest  # type: ignore
from typing_extensions import (
    Literal,
    TypeAlias,
)

from neptune.api.field_visitor import FieldToValueVisitor
from neptune.api.models import (
    Field,
    FieldType,
    LeaderboardEntriesSearchResult,
    LeaderboardEntry,
)
from neptune.api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import ProtoLeaderboardEntriesSearchResultDTO
from neptune.exceptions import NeptuneInvalidQueryException
from neptune.internal.backends.hosted_client import (
    DEFAULT_PROTO_REQUEST_KWARGS,
    DEFAULT_REQUEST_KWARGS,
)
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


SUPPORTED_ATTRIBUTE_TYPES = {item.value for item in FieldType}

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
    use_proto: Optional[bool] = None,
) -> LeaderboardEntriesSearchResult:
    normalized_query = query or NQLEmptyQuery()
    sort_by_column_type = sort_by_column_type if sort_by_column_type else FieldType.STRING.value
    if sort_by and searching_after:
        sort_by_as_nql = NQLQueryAttribute(
            name=sort_by,
            type=NQLAttributeType(sort_by_column_type),
            operator=NQLAttributeOperator.GREATER_THAN if ascending else NQLAttributeOperator.LESS_THAN,
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
                    "type": sort_by_column_type if sort_by_column_type else FieldType.STRING.value,
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

    try:
        if use_proto:
            result = (
                client.api.searchLeaderboardEntriesProto(**params, **DEFAULT_PROTO_REQUEST_KWARGS).response().result
            )
            proto_data = ProtoLeaderboardEntriesSearchResultDTO.FromString(result)
            return LeaderboardEntriesSearchResult.from_proto(proto_data)
        else:
            model_data = client.api.searchLeaderboardEntries(**params, **DEFAULT_REQUEST_KWARGS).response().result
            return LeaderboardEntriesSearchResult.from_model(model_data)
    except HTTPBadRequest as e:
        title = e.response.json().get("title")
        if title == "Syntax error":
            raise NeptuneInvalidQueryException(nql_query=str(normalized_query))
        raise e


def find_attribute(*, entry: LeaderboardEntry, path: str) -> Optional[Field]:
    return next((attr for attr in entry.fields if attr.path == path), None)


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

    data = get_single_page(
        limit=0,
        offset=0,
        sort_by=sort_by,
        ascending=ascending,
        sort_by_column_type=sort_by_column_type,
        searching_after=None,
        **kwargs,
    )
    total = data.matching_item_count

    limit = limit if limit is not None else NoLimit()

    total = total if total < limit else limit

    progress_bar = False if total <= step_size else progress_bar  # disable progress bar if only one page is fetched

    extracted_records = 0

    field_to_value_visitor = FieldToValueVisitor()

    with construct_progress_bar(progress_bar, "Fetching table...") as bar:
        # beginning of the first page
        bar.update(
            by=0,
            total=total,
        )

        while True:
            if last_page:
                searching_after_field = find_attribute(entry=last_page[-1], path=sort_by)
                if not searching_after_field:
                    raise ValueError(f"Cannot find attribute {sort_by} in last page")
                searching_after = field_to_value_visitor.visit(searching_after_field)

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
                    total += result.matching_item_count

                total = min(total, limit)

                page = result.entries
                extracted_records += len(page)
                bar.update(by=len(page), total=total)

                if not page:
                    return

                yield from page

                if extracted_records == limit:
                    return

                last_page = page
