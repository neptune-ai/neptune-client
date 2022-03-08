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
from neptune.new.internal.init.project import get_project, init_project
from neptune.new.internal.init.run import init_run
from neptune.new.internal.init.model import init_model
from neptune.new.internal.init.model_version import init_model_version
from neptune.new.types.mode import Mode

init = init_run
RunMode = Mode
