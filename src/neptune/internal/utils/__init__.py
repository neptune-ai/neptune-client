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
__all__ = [
    "replace_patch_version",
    "verify_type",
    "verify_value",
    "is_stream",
    "is_bool",
    "is_int",
    "is_float",
    "is_string",
    "is_float_like",
    "is_dict_like",
    "is_string_like",
    "is_stringify_value",
    "verify_collection_type",
    "verify_optional_callable",
    "is_collection",
    "base64_encode",
    "base64_decode",
    "get_absolute_paths",
    "get_common_root",
    "does_paths_share_common_drive",
    "is_ipython",
    "as_list",
]

import base64
import os
from glob import glob
from io import IOBase
from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    TypeVar,
    Union,
)

from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils.logger import get_logger

T = TypeVar("T")

_logger = get_logger()


def replace_patch_version(version: str):
    return version[: version.index(".", version.index(".") + 1)] + ".0"


def verify_type(var_name: str, var, expected_type: Union[type, tuple]):
    try:
        if isinstance(expected_type, tuple):
            type_name = " or ".join(get_type_name(t) for t in expected_type)
        else:
            type_name = get_type_name(expected_type)
    except Exception as e:
        # Just to be sure that nothing weird will be raised here
        raise TypeError("Incorrect type of {}".format(var_name)) from e

    if not isinstance(var, expected_type):
        raise TypeError("{} must be a {} (was {})".format(var_name, type_name, type(var)))

    if isinstance(var, IOBase) and not hasattr(var, "read"):
        raise TypeError("{} is a stream, which does not implement read method".format(var_name))


def verify_value(var_name: str, var: Any, expected_values: Iterable[T]) -> None:
    if var not in expected_values:
        raise ValueError(f"{var_name} must be one of {expected_values} (was `{var}`)")


def is_stream(var):
    return isinstance(var, IOBase) and hasattr(var, "read")


def is_bool(var):
    return isinstance(var, bool)


def is_int(var):
    return isinstance(var, int)


def is_float(var):
    return isinstance(var, (float, int))


def is_string(var):
    return isinstance(var, str)


def is_float_like(var):
    try:
        _ = float(var)
        return True
    except (ValueError, TypeError):
        return False


def is_dict_like(var):
    return isinstance(var, (dict, Mapping))


def is_string_like(var):
    try:
        _ = str(var)
        return True
    except ValueError:
        return False


def is_stringify_value(var):
    return isinstance(var, StringifyValue)


def get_type_name(_type: Union[type, tuple]):
    return _type.__name__ if hasattr(_type, "__name__") else str(_type)


def verify_collection_type(var_name: str, var, expected_type: Union[type, tuple]):
    verify_type(var_name, var, (list, set, tuple))
    for value in var:
        verify_type("elements of collection '{}'".format(var_name), value, expected_type)


def verify_optional_callable(var_name: str, var):
    if var and not callable(var):
        raise TypeError("{} must be a callable (was {})".format(var_name, type(var)))


def is_collection(var) -> bool:
    return isinstance(var, (list, set, tuple))


def base64_encode(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


def base64_decode(data: str) -> bytes:
    return base64.b64decode(data.encode("utf-8"))


def get_absolute_paths(file_globs: Iterable[str]) -> List[str]:
    expanded_paths: Set[str] = set()
    for file_glob in file_globs:
        expanded_paths |= set(glob(file_glob, recursive=True))
    return list(os.path.abspath(expanded_file) for expanded_file in expanded_paths)


def get_common_root(absolute_paths: List[str]) -> Optional[str]:
    try:
        common_root = os.path.commonpath(absolute_paths)
        if os.path.isfile(common_root):
            common_root = os.path.dirname(common_root)
        if common_root.startswith(os.getcwd() + os.sep):
            common_root = os.getcwd()
        return common_root
    except ValueError:
        return None


def does_paths_share_common_drive(paths: List[str]) -> bool:
    return len(set(map(lambda path: os.path.splitdrive(path)[0], paths))) == 1


def is_ipython() -> bool:
    try:
        import IPython

        ipython = IPython.core.getipython.get_ipython()
        return ipython is not None
    except ImportError:
        return False


def as_list(name: str, value: Optional[Union[str, Iterable[str]]]) -> Iterable[str]:
    verify_type(name, value, (type(None), str, Iterable))

    if value is None:
        return []

    if isinstance(value, str):
        return [value]

    verify_collection_type(name, value, str)

    return value
