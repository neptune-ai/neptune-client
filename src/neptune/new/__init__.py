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

from typing import Optional

from neptune.new import types
from neptune.new.constants import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
)
from neptune.new.exceptions import (
    NeptunePossibleLegacyUsageException,
    NeptuneUninitializedException,
)
from neptune.new.internal.init import (
    get_project,
    init,
    init_model,
    init_model_version,
    init_project,
    init_run,
)
from neptune.new.internal.utils.deprecation import deprecated
from neptune.new.metadata_containers import Run
from neptune.new.version import version

__version__ = str(version)


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


@deprecated()
def get_last_run() -> Optional[Run]:
    """Returns last created Run object.

    Returns:
        ``Run``: object last created by neptune global object.

    Examples:
        >>> import neptune.new as neptune

        >>> # Crate a new tracked run
        ... neptune.init_run(name='A new approach', source_files='**/*.py')
        ... # Oops! We didn't capture the reference to the Run object

        >>> # Not a problem! We've got you covered.
        ... run = neptune.get_last_run()

    You may also want to check `get_last_run docs page`_.

    .. _get_last_run docs page:
       https://docs.neptune.ai/api/neptune/#get_last_run
    """
    last_run = Run.last_run
    if last_run is None:
        raise NeptuneUninitializedException()
    return last_run
