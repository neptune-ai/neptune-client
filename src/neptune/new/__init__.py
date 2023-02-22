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
    "ANONYMOUS_API_TOKEN",
    "NeptunePossibleLegacyUsageException",
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
]

import sys

from neptune.new._compatibility import CompatibilityImporter

sys.meta_path.insert(0, CompatibilityImporter())

from neptune import (
    ANONYMOUS_API_TOKEN,
    Run,
    __version__,
    init_model,
    init_model_version,
    init_project,
    init_run,
)
from neptune.common.warnings import warn_once
from neptune.new.attributes import *
from neptune.new.cli import *
from neptune.new.constants import *
from neptune.new.envs import *
from neptune.new.exceptions import *
from neptune.new.handler import *
from neptune.new.integrations import *
from neptune.new.logging import *
from neptune.new.metadata_containers import *
from neptune.new.project import *
from neptune.new.run import *
from neptune.new.runs_table import *
from neptune.new.types import *
from neptune.new.utils import *


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


warn_once(
    message="You're importing the Neptune client library via the deprecated"
    " `neptune.new` module, which will be removed in a future release."
    " Import directly from `neptune` instead."
)
