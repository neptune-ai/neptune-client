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
__all__ = [
    "ANONYMOUS_API_TOKEN",
    "NEPTUNE_DATA_DIRECTORY",
    "OFFLINE_DIRECTORY",
    "ASYNC_DIRECTORY",
    "SYNC_DIRECTORY",
    "OFFLINE_NAME_PREFIX",
    "MAX_32_BIT_INT",
    "MIN_32_BIT_INT",
]

"""Constants used by Neptune"""


ANONYMOUS_API_TOKEN = "ANONYMOUS"

NEPTUNE_DATA_DIRECTORY = ".neptune"

OFFLINE_DIRECTORY = "offline"
ASYNC_DIRECTORY = "async"
SYNC_DIRECTORY = "sync"

OFFLINE_NAME_PREFIX = "offline/"

MAX_32_BIT_INT = 2147483647
MIN_32_BIT_INT = -2147483648
