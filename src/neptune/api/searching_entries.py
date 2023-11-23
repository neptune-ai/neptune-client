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
__all__ = ["search_leaderboard_entries"]

import os
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
)

from bravado.exception import HTTPNotFound  # type: ignore

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.envs import NEPTUNE_FETCH_TABLE_STEP_SIZE
from neptune.exceptions import ProjectNotFound
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS

if TYPE_CHECKING:
    from neptune.internal.backends.nql import NQLQuery
    from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


@with_api_exceptions_handler
def search_leaderboard_entries(
    *,
    swagger_client: "SwaggerClientWrapper",
    project_id: "UniqueId",
    types: Optional[Iterable["ContainerType"]] = None,
    query: Optional["NQLQuery"] = None,
    columns: Optional[Iterable[str]] = None,
) -> List[LeaderboardEntry]:
    step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "100"))

    if query:
        query_params = {"query": {"query": str(query)}}
    else:
        query_params = {}

    if columns:
        attributes_filter = {"attributeFilters": [{"path": column} for column in columns]}
    else:
        attributes_filter = {}

    types_filter = list(map(lambda container_type: container_type.to_api(), types)) if types else None

    try:
        return [
            to_leaderboard_entry(entry=entry)
            for entry in iter_over_pages(
                iter_once=partial(
                    get_single_page,
                    client=swagger_client,
                    project_id=project_id,
                    types=types_filter,
                    query_params=query_params,
                    attributes_filter=attributes_filter,
                ),
                step=step_size,
            )
        ]
    except HTTPNotFound:
        raise ProjectNotFound(project_id)


def get_single_page(
    *,
    client: "SwaggerClientWrapper",
    project_id: "UniqueId",
    query_params: Dict[str, Any],
    attributes_filter: Dict[str, Any],
    limit: int,
    offset: int,
    types: Optional[Iterable[str]] = None,
) -> List[Any]:
    return list(
        client.api.searchLeaderboardEntries(
            projectIdentifier=project_id,
            type=types,
            params={
                **query_params,
                **attributes_filter,
                "pagination": {"limit": limit, "offset": offset},
            },
            **DEFAULT_REQUEST_KWARGS,
        )
        .response()
        .result.entries
    )


def to_leaderboard_entry(*, entry: Any) -> LeaderboardEntry:
    supported_attribute_types = {item.value for item in AttributeType}
    attributes: List[AttributeWithProperties] = []

    for attr in entry.attributes:
        if attr.type in supported_attribute_types:
            properties = attr.__getitem__(f"{attr.type}Properties")
            attributes.append(
                AttributeWithProperties(path=attr.name, type=AttributeType(attr.type), properties=properties)
            )

    return LeaderboardEntry(id=entry.experimentId, attributes=attributes)


def iter_over_pages(*, iter_once: Callable[..., List[Any]], step: int, max_server_offset: int = 10000) -> List[Any]:
    items: List[Any] = []
    previous_items = None
    while (previous_items is None or len(previous_items) >= step) and len(items) < max_server_offset:
        previous_items = iter_once(limit=step, offset=len(items))
        items += previous_items

    return items
