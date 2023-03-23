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
"""Log and organize all your ML model metadata in a single place.

The neptune package contains the functions needed to initialize Neptune objects.
These help you track, store, and visualize metadata related to your model-training experiments.
You can organize your metadata per experiment, model, model version, or project.

Functions:
    init_run()
    init_model()
    init_model_version()
    init_project()

Constants:
    ANONYMOUS_API_TOKEN

Runs
----
The run will track some things automatically during the execution of your model training
script, such as hardware consumption, source code, and Git information. You can also
assign any metadata to the run manually and organize it in a structure of your choosing.

>>> run = neptune.init_run()
>>> run["some/structure"] = some_metadata

Model registry
--------------
Create a model object for a model you're working on:

>>> model = neptune.init_model(key="KEY")
>>> model["signature"].upload("signature.json")

Create as many versions of the model as you need, tracking their metadata and
lifecycles separately:

>>> model_version = neptune.init_model_version(model="???-KEY")
>>> model_version["dataset_version"].track_files("./data/train.csv")
>>> model_version.change_stage("staging")

Project-level metadata
----------------------
Initialize your entire Neptune project and log metadata on project-level:

>>> project = neptune.init_project(project="ml-team/classification")
>>> project["datasets"].upload("./data/")

Anonymous logging
-----------------
To try out Neptune without registering, you can use the "ANONYMOUS_API_TOKEN" when
initializing Neptune.

>>> run = neptune.init_run(api_token=neptune.ANONYMOUS_API_TOKEN)

---

Learn more in the docs: https://docs.neptune.ai/api/neptune/
"""
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
from neptune.metadata_containers import (
    Model,
    ModelVersion,
    Project,
    Run,
)
from neptune.version import __version__

# Apply patches of external libraries
apply_patches()

init_run = Run
init_model = Model
init_model_version = ModelVersion
init_project = Project
