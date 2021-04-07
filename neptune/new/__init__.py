#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from typing import Optional

from neptune.new import types
from neptune.new.constants import ANONYMOUS, ANONYMOUS_API_TOKEN
from neptune.new.exceptions import NeptuneUninitializedException
from neptune.new.run import Run
from neptune.new.internal.get_project_impl import get_project
from neptune.new.internal.init_impl import __version__, init


def get_last_run() -> Optional[Run]:
    last_run = Run.last_run
    if last_run is None:
        raise NeptuneUninitializedException()
    return last_run
