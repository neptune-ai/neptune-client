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
__all__ = ("ReadOnlyOperationProcessor",)

from typing import TYPE_CHECKING

from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.internal.operation_processors.operation_processor import OperationProcessor

if TYPE_CHECKING:
    from neptune.internal.operation import Operation


class ReadOnlyOperationProcessor(OperationProcessor):
    def enqueue_operation(self, op: "Operation", *, wait: bool) -> None:
        warn_once("Client in read-only mode, nothing will be saved to server.", exception=NeptuneWarning)
