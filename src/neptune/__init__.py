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
    "init_model",
    "init_model_version",
    "init_project",
    "init_run",
    "Run",
    "Model",
    "ModelVersion",
    "Project",
    "__version__",
]


from neptune.common.patches import apply_patches
from neptune.constants import ANONYMOUS_API_TOKEN
from neptune.internal.init.model import init_model
from neptune.internal.init.model_version import init_model_version
from neptune.internal.init.project import init_project
from neptune.internal.init.run import init_run
from neptune.metadata_containers import (
    Model,
    ModelVersion,
    Project,
    Run,
)
from neptune.version import version

__version__ = str(version)

# TODO: init_model = Model
# TODO: init_model_version = ModelVersion
# TODO: init_project = Project
# TODO: init_run = Run


# Apply patches of external libraries
apply_patches()
