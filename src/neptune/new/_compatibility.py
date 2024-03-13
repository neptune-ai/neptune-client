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
from importlib.machinery import ModuleSpec


class CompatibilityModuleLoader(Loader):
    def exec_module(self, module):
        fullname = module.__name__
        module_name_parts = fullname.split(".")
        new_module_name = f"neptune.{module_name_parts[2]}"

        # Load the module with the new name and update sys.modules
        new_module = import_module(new_module_name)
        sys.modules[fullname] = new_module

        # Update the module's dictionary to reflect the newly loaded module
        module.__dict__.update(new_module.__dict__)


modules = [
    "neptune.new.attributes",
    "neptune.new.cli",
    "neptune.new.integrations",
    "neptune.new.logging",
    "neptune.new.metadata_containers",
    "neptune.new.types",
]


class CompatibilityImporter(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in modules:
            return ModuleSpec(fullname, CompatibilityModuleLoader(), is_package=False)
        return None  # Not handling other modules
