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
__all__ = ["CompatibilityImporter"]

import sys
from importlib import import_module
from importlib.abc import (
    Loader,
    MetaPathFinder,
)


class CompatibilityModuleLoader(Loader):
    def module_repr(self, module):
        return repr(module)

    def load_module(self, fullname):
        module_name_parts = fullname.split(".")
        new_module_name = f"neptune.{module_name_parts[2]}"

        module = sys.modules[fullname] = import_module(new_module_name)
        return module


modules = [
    "neptune.new.attributes",
    "neptune.new.cli",
    "neptune.new.integrations",
    "neptune.new.logging",
    "neptune.new.metadata_containers",
    "neptune.new.types",
]


class CompatibilityImporter(MetaPathFinder):
    def find_module(self, fullname, path=None):
        if ".".join(fullname.split(".")) in modules:
            return CompatibilityModuleLoader()
