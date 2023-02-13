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
# flake8: noqa
__all__ = [
    "ANONYMOUS",
    "ANONYMOUS_API_TOKEN",
    "NeptunePossibleLegacyUsageException",
    "NeptuneUninitializedException",
    "get_project",
    "init",
    "init_model",
    "init_model_version",
    "init_project",
    "init_run",
    "Run",
    "__version__",
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
    "get_last_run",
]

import sys

from neptune import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
    Run,
    __version__,
    get_last_run,
    get_project,
    init,
    init_model,
    init_model_version,
    init_project,
    init_run,
)
from neptune.attributes import *
from neptune.cli import *
from neptune.common.deprecation import warn_once
from neptune.exceptions import (
    NeptunePossibleLegacyUsageException,
    NeptuneUninitializedException,
)
from neptune.integrations import *
from neptune.logging import *
from neptune.metadata_containers import *
from neptune.new._compatibility import CompatibilityImporter
from neptune.types import *


def _raise_legacy_client_expected(*args, **kwargs):
    raise NeptunePossibleLegacyUsageException()


create_experiment = (
    get_experiment
) = (
    append_tag
) = (
    append_tags
) = (
    remove_tag
) = (
    set_property
) = (
    remove_property
) = (
    send_metric
) = (
    log_metric
) = (
    send_text
) = (
    log_text
) = send_image = log_image = send_artifact = delete_artifacts = log_artifact = stop = _raise_legacy_client_expected


sys.meta_path.append(CompatibilityImporter())

warn_once(
    message="You're importing neptune client via deprecated `neptune.new`"
    " module and it will be removed in a future."
    " Try to import it directly from `neptune`."
)
