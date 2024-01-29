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
"""Log and organize all your ML model metadata with neptune.ai.

There are four kinds of Neptune objects: run, model, model version, and project.
They help you track, store, and visualize metadata related to your model-training experiments.
The package contains the functions and constructors needed to initialize the objects.
You can either create new objects or connect to existing ones (to, for example, fetch or add more metadata).

Functions:
    init_run()
    init_model()
    init_model_version()
    init_project()

Classes:
    Run
    Model
    ModelVersion
    Project

Constants:
    ANONYMOUS_API_TOKEN

Tracking runs
-------------
A Neptune run tracks some things automatically during the execution of your model training
script, such as hardware consumption, source code, and Git information. You can also
assign any metadata to the run manually and organize it in a structure of your choosing.

>>> run = neptune.init_run()
>>> run["some/structure"] = some_metadata

Model registry
--------------
Create a model object to register a model:

>>> model = neptune.init_model(key="MOD")
>>> model["signature"].upload("signature.json")

Then create as many versions of the model as you need, tracking their metadata and
lifecycles separately:

>>> model_version = neptune.init_model_version(model="PROJ-MOD")
>>> model_version["dataset_version"].track_files("./data/train.csv")
>>> model_version.change_stage("staging")

Project metadata
----------------
Initialize your entire Neptune project and log metadata on project-level:

>>> project = neptune.init_project(project="ml-team/classification")
>>> project["datasets"].upload("./data/")

Initializing with class constructor
-----------------------------------
You can also use the class constructor to initialize a Neptune object.

>>> from neptune import Run
>>> run = Run()

>>> from neptune import ModelVersion
>>> model_version = ModelVersion(with_id="PROJ-MOD-3")  # connect to existing model version
>>> model_version.change_stage("production")

Anonymous logging
-----------------
To try out Neptune without registering, you can pass the `ANONYMOUS_API_TOKEN` constant
to the `api_token` argument when initializing Neptune.

>>> with neptune.init_run(api_token=neptune.ANONYMOUS_API_TOKEN) as run:
...     ...

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
from neptune.internal.extensions import load_extensions
from neptune.metadata_containers import (
    Model,
    ModelVersion,
    Project,
    Run,
)
from neptune.version import __version__

# Apply patches of external libraries
apply_patches()
load_extensions()

init_run = Run
init_model = Model
init_model_version = ModelVersion
init_project = Project
