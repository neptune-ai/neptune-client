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
__all__ = ["stringify_unsupported"]

from typing import (
    Any,
    List,
    Mapping,
    Tuple,
    Union,
)

from neptune.new.internal.utils.stringify_value import StringifyValue


def stringify_unsupported(value: Any) -> Union[StringifyValue, Mapping, List, Tuple]:
    """Helper function that converts unsupported values in a collection or dictionary to strings.
    Args:
        value (Any): A dictionary with values or a collection
    Example:
        >>> import neptune.new as neptune
        >>> run = neptune.init_run()
        >>> complex_dict = {"tuple": ("hi", 1), "metric": 0.87}
        >>> run["complex_dict"] = complex_dict
        >>> # (as of 1.0.0) error - tuple is not a supported type
        ... from neptune.new.utils import stringify_unsupported
        >>> run["complex_dict"] = stringify_unsupported(complex_dict)

        For more information, see:
        https://docs.neptune.ai/setup/neptune-client_1-0_release_changes/#no-more-implicit-casting-to-string
    """
    if isinstance(value, dict):
        return {k: stringify_unsupported(v) for k, v in value.items()}

    if isinstance(value, list):
        return list(map(stringify_unsupported, value))

    if isinstance(value, tuple):
        return tuple(map(stringify_unsupported, value))

    return StringifyValue(value=value)
