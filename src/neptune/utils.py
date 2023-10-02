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
"""Utility functions to support ML metadata logging with neptune.ai."""
__all__ = ["stringify_unsupported", "stop_synchronization_callback"]

from typing import (
    Any,
    Mapping,
    MutableMapping,
    Union,
)

from neptune.internal.init.parameters import DEFAULT_STOP_TIMEOUT
from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils.logger import logger
from neptune.typing import NeptuneObject


def stringify_unsupported(value: Any) -> Union[StringifyValue, Mapping]:
    """Helper function that converts unsupported values in a collection or dictionary to strings.

    Args:
        value (Any): A dictionary with values or a collection

    Example:
        >>> import neptune
        >>> run = neptune.init_run()
        >>> complex_dict = {"tuple": ("hi", 1), "metric": 0.87}
        >>> run["complex_dict"] = complex_dict
        >>> # (as of 1.0.0) error - tuple is not a supported type
        ... from neptune.utils import stringify_unsupported
        >>> run["complex_dict"] = stringify_unsupported(complex_dict)

        For more information, see:
        https://docs.neptune.ai/setup/neptune-client_1-0_release_changes/#no-more-implicit-casting-to-string
    """
    if isinstance(value, MutableMapping):
        return {str(k): stringify_unsupported(v) for k, v in value.items()}

    return StringifyValue(value=value)


def stop_synchronization_callback(neptune_object: NeptuneObject) -> None:
    """Default callback function that stops a Neptune object's synchronization with the server.

    Args:
        neptune_object: A Neptune object (Run, Model, ModelVersion, or Project) to be stopped.

    Example:
        >>> import neptune
        >>> from neptune.utils import stop_synchronization_callback
        >>> run = neptune.init_run(
        ...     async_no_progress_callback = stop_synchronization_callback
        ... )

    For more information, see:
        https://docs.neptune.ai/api/utils/stop_synchronization_callback/
    """
    logger.error(
        "Threshold for disrupted synchronization exceeded. Stopping the synchronization using the default callback."
    )
    neptune_object.stop(seconds=DEFAULT_STOP_TIMEOUT)
