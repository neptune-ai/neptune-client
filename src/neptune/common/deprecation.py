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
__all__ = ["warn_once", "NeptuneDeprecationWarning"]

import os
import traceback
import warnings

import neptune


class NeptuneDeprecationWarning(DeprecationWarning):
    pass


warnings.simplefilter("always", category=NeptuneDeprecationWarning)

warned_once = set()
path_to_root_module = os.path.dirname(os.path.realpath(neptune.__file__))


def get_user_code_stack_level():
    call_stack = traceback.extract_stack()
    for level, stack_frame in enumerate(reversed(call_stack)):
        if path_to_root_module not in stack_frame.filename:
            return level
    return 2


def warn_once(message: str):
    if message not in warned_once:
        warnings.warn(
            message=message,
            category=NeptuneDeprecationWarning,
            stacklevel=get_user_code_stack_level(),
        )
        warned_once.add(message)
