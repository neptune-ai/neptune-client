#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["SafeContainer"]

from typing import (
    Any,
    Callable,
)

from neptune.internal.utils.logger import logger
from neptune.metadata_containers import MetadataContainer


class SafeMethodWrapper:
    def __init__(self, method: Callable):
        self._method = method

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return self._method(*args, **kwargs)
        except Exception:
            logger.exception(f"Exception in method {self._method}")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._api_method, item)


class SafeContainer:
    def __init__(self, container: MetadataContainer):
        self._container = container

    def __getattr__(self, item: str) -> Any:
        return SafeMethodWrapper(getattr(self._container, item))
