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
    "ANONYMOUS",
    "ANONYMOUS_API_TOKEN",
    "get_project",
    "init",
    "init_model",
    "init_model_version",
    "init_project",
    "init_run",
    "Run",
    "__version__",
    "get_last_run",
]

from typing import Optional

from neptune.common.patches import apply_patches
from neptune.constants import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
)
from neptune.exceptions import NeptuneUninitializedException
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
from neptune.version import version


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


__version__ = str(version)


# Apply patches of external libraries
apply_patches()
