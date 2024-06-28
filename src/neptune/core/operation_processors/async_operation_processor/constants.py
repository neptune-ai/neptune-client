#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

__all__ = ["STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS", "STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS"]

import os

from neptune.envs import NEPTUNE_SYNC_AFTER_STOP_TIMEOUT
from neptune.internal.parameters import DEFAULT_STOP_TIMEOUT

STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30.0
STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = float(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))
