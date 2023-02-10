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

"""``neptune`` is a global object that you can use to start new tracked runs or re-connect to already existing ones.

It also provides some convenience functionalities like obtaining the last created run.

You may also want to check `Neptune docs page`_.

.. _Neptune docs page:
   https://docs.neptune.ai/api/neptune
"""
__all__ = [
    "types",
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

from neptune import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
    NeptunePossibleLegacyUsageException,
    NeptuneUninitializedException,
    Run,
    __version__,
    append_tag,
    append_tags,
    create_experiment,
    delete_artifacts,
    get_experiment,
    get_last_run,
    get_project,
    init,
    init_model,
    init_model_version,
    init_project,
    init_run,
    log_artifact,
    log_image,
    log_metric,
    log_text,
    remove_property,
    remove_tag,
    send_artifact,
    send_image,
    send_metric,
    send_text,
    set_property,
    stop,
    types,
)
