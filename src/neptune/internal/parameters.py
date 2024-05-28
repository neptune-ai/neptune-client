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
__all__ = [
    "DEFAULT_FLUSH_PERIOD",
    "DEFAULT_NAME",
    "OFFLINE_PROJECT_QUALIFIED_NAME",
    "ASYNC_LAG_THRESHOLD",
    "ASYNC_NO_PROGRESS_THRESHOLD",
    "DEFAULT_STOP_TIMEOUT",
    "MAX_SERVER_OFFSET",
    "IN_BETWEEN_CALLBACKS_MINIMUM_INTERVAL",
]

DEFAULT_FLUSH_PERIOD = 5
DEFAULT_NAME = "Untitled"
OFFLINE_PROJECT_QUALIFIED_NAME = "offline/project-placeholder"
ASYNC_LAG_THRESHOLD = 1800.0
ASYNC_NO_PROGRESS_THRESHOLD = 300.0
DEFAULT_STOP_TIMEOUT = 60.0
IN_BETWEEN_CALLBACKS_MINIMUM_INTERVAL = 300.0
MAX_SERVER_OFFSET = 10_000
