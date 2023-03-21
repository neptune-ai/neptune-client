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
from pathlib import Path
from random import choices
from string import ascii_lowercase

from neptune.vendor.lib_programname import (
    empty_path,
    get_valid_executable_path_or_empty_path,
)


def test__non_existent():
    # given
    arg_string = "some/path/to/executable.py"

    # when
    found_path = get_valid_executable_path_or_empty_path(arg_string)

    # then
    assert found_path == empty_path


def test__exists():
    # given
    arg_string = __file__

    # when
    found_path = get_valid_executable_path_or_empty_path(arg_string)

    # then
    assert found_path == Path(arg_string).resolve()


def test__too_long_name():
    # given
    arg_string = "".join(choices(ascii_lowercase, k=1024))

    # when
    found_path = get_valid_executable_path_or_empty_path(arg_string)

    # then
    assert found_path == empty_path
