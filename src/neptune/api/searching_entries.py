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
    Generator,
    Iterable,
    List,
    Optional,
)

from bravado.client import construct_request  # type: ignore
from bravado.config import RequestConfig  # type: ignore
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


SUPPORTED_ATTRIBUTE_TYPES = {item.value for item in AttributeType}


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

    types_filter = list(map(lambda container_type: container_type.to_api(), types)) if types else None
    query_params = {"query": {"query": str(query)}} if query else {}
    attributes_filter = {"attributeFilters": [{"path": column} for column in columns]} if columns else {}

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
    params = {
        "projectIdentifier": project_id,
        "type": types,
        "params": {
            **query_params,
            **attributes_filter,
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

    return list(result.get("entries", []))


def to_leaderboard_entry(*, entry: Dict[str, Any]) -> LeaderboardEntry:
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


def iter_over_pages(
    *, iter_once: Callable[..., List[Any]], step: int, max_server_offset: int = 10000
) -> Generator[Any, None, None]:
    previous_items = None
    num_of_collected_items = 0

    while (previous_items is None or len(previous_items) >= step) and num_of_collected_items < max_server_offset:
        previous_items = iter_once(limit=step, offset=num_of_collected_items)
        num_of_collected_items += len(previous_items)
        yield from previous_items
