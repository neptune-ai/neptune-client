#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import base64
from typing import Union, List, Set, Tuple, TypeVar

T = TypeVar('T')
Collection = Union[List[T], Set[T], Tuple[T]]


def replace_patch_version(version: str):
    return version[:version.index(".", version.index(".") + 1)] + ".0"


def verify_type(var_name: str, var, expected_type: Union[type, tuple]):
    try:
        type_name = (" or ".join(t.__name__ for t in expected_type)
                     if isinstance(expected_type, tuple)
                     else expected_type.__name__)
    except Exception as e:
        # Just to be sure that nothing weird will be raised here
        raise TypeError("Incorrect type of {}".format(var_name)) from e

    if not isinstance(var, expected_type):
        raise TypeError("{} must be a {} (was {})".format(var_name, type_name, type(var)))


def verify_collection_type(var_name: str, var, expected_type: Union[type, tuple]):
    verify_type(var_name, var, (list, set, tuple))
    for value in var:
        verify_type("elements of {}".format(var_name), value, expected_type)


def base64_encode(data: bytes) -> str:
    return base64.b64encode(data).decode('utf-8')
