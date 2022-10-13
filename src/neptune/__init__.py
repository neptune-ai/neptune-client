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
from neptune._version import get_versions
from neptune.legacy import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
    InvalidNeptuneBackend,
    NeptuneIncorrectImportException,
    NeptuneUninitializedException,
    Project,
    Session,
    api_exceptions,
    append_tag,
    append_tags,
    assure_project_qualified_name,
    backend,
    backend_factory,
    checkpoint,
    constants,
    create_experiment,
    delete_artifacts,
    envs,
    exceptions,
    experiments,
    get_experiment,
    git_info,
    init,
    log_artifact,
    log_image,
    log_metric,
    log_text,
    model,
    notebook,
    oauth,
    patterns,
    project,
    projects,
    remove_property,
    remove_tag,
    send_artifact,
    send_image,
    send_metric,
    send_text,
    session,
    sessions,
    set_project,
    set_property,
    stop,
    utils,
)

__version__ = get_versions()["version"]

__all__ = [
    "__version__",
    "backend_factory",
    "Project",
    "Session",
    "assure_project_qualified_name",
    "InvalidNeptuneBackend",
    "NeptuneIncorrectImportException",
    "NeptuneUninitializedException",
    "api_exceptions",
    "backend",
    "checkpoint",
    "constants",
    "envs",
    "exceptions",
    "experiments",
    "git_info",
    "model",
    "notebook",
    "oauth",
    "patterns",
    "projects",
    "sessions",
    "utils",
    "session",
    "project",
    "ANONYMOUS",
    "ANONYMOUS_API_TOKEN",
    "init",
    "set_project",
    "create_experiment",
    "get_experiment",
    "append_tag",
    "append_tags",
    "remove_tag",
    "set_property",
    "remove_property",
    "send_metric",
    "log_metric",
    "send_text",
    "log_text",
    "send_image",
    "log_image",
    "send_artifact",
    "delete_artifacts",
    "log_artifact",
    "stop",
]
