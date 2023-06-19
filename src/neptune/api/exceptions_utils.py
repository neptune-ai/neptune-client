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
__all__ = ["handle_json_errors"]

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)


def handle_json_errors(
    content: Dict[str, Any],
    source_exception: Exception,
    error_processors: Dict[str, Callable[[Dict[str, Any]], Exception]],
    default_exception: Optional[Exception] = None,
) -> None:
    error_type: Optional[str] = content.get("errorType")
    error_processor = error_processors.get(error_type)

    if error_processor:
        raise error_processor(content) from source_exception
    elif default_exception:
        raise default_exception from source_exception
    raise source_exception
