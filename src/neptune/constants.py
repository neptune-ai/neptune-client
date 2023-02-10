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
    "ANONYMOUS",
    "ANONYMOUS_API_TOKEN",
    "NEPTUNE_DATA_DIRECTORY",
    "NEPTUNE_RUNS_DIRECTORY",
    "OFFLINE_DIRECTORY",
    "ASYNC_DIRECTORY",
    "SYNC_DIRECTORY",
    "OFFLINE_NAME_PREFIX",
]

"""Constants used by Neptune"""

ANONYMOUS = "ANONYMOUS"

ANONYMOUS_API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLm5lcHR1bmUuYWkiLCJhcGlfdXJsIjoiaHR0cHM6Ly9hcHAubmVwdHVuZS"
    "5haSIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQo="
)

NEPTUNE_DATA_DIRECTORY = ".neptune"
# backwards compat
NEPTUNE_RUNS_DIRECTORY = NEPTUNE_DATA_DIRECTORY

OFFLINE_DIRECTORY = "offline"
ASYNC_DIRECTORY = "async"
SYNC_DIRECTORY = "sync"

OFFLINE_NAME_PREFIX = "offline/"
