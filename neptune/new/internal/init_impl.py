#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
from neptune.new.internal.init_project import get_project, init_project
from neptune.new.internal.init_run import __version__, init_run
from neptune.new.types.mode import Mode

init = init_run
RunMode = Mode
__all__ = ["__version__", "get_project", "init_project", "init_run", "init", "RunMode"]
