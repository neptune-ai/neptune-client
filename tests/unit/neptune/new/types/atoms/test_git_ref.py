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
from pathlib import Path

from neptune.types import GitRef
from neptune.vendor.lib_programname import get_path_executed_script


class TestGitRef:
    def test_resolve_path_default(self):
        assert GitRef().resolve_path() == get_path_executed_script().resolve()

    def test_resolve_path_provided(self):
        assert GitRef("path").resolve_path() == Path("path").resolve()
        assert GitRef(Path("path")).resolve_path() == Path("path").resolve()
