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
    "warn_once",
    "warn_about_unsupported_type",
    "NeptuneDeprecationWarning",
    "NeptuneWarning",
    "NeptuneUnsupportedType",
    "NeptuneUnsupportedValue",
]

import os
import traceback
import warnings

import neptune
from neptune.internal.utils.logger import NEPTUNE_LOGGER_NAME
from neptune.internal.utils.runningmode import in_interactive

DEFAULT_FORMAT = "[%(name)s] [warning] %(filename)s:%(lineno)d: %(category)s: %(message)s\n"
INTERACTIVE_FORMAT = "[%(name)s] [warning] %(category)s: %(message)s\n"


class NeptuneDeprecationWarning(DeprecationWarning):
    pass


class NeptuneUnsupportedValue(Warning):
    pass


class NeptuneWarning(Warning):
    pass


class NeptuneUnsupportedType(Warning):
    pass


warnings.simplefilter("always", category=NeptuneDeprecationWarning)

MAX_WARNED_ONCE_CAPACITY = 1_000
warned_once = set()
path_to_root_module = os.path.dirname(os.path.realpath(neptune.__file__))


def get_user_code_stack_level():
    call_stack = traceback.extract_stack()
    for level, stack_frame in enumerate(reversed(call_stack)):
        if path_to_root_module not in stack_frame.filename:
            return level
    return 2


def format_message(message, category, filename, lineno, line=None) -> str:
    variables = {
        "message": message,
        "category": category.__name__,
        "filename": filename,
        "lineno": lineno,
        "name": NEPTUNE_LOGGER_NAME,
    }

    message_format = INTERACTIVE_FORMAT if in_interactive() else DEFAULT_FORMAT

    return message_format % variables


def warn_once(message: str, *, exception: type(Exception) = None):
    if len(warned_once) < MAX_WARNED_ONCE_CAPACITY:
        if exception is None:
            exception = NeptuneDeprecationWarning

        message_hash = hash(message)

        if message_hash not in warned_once:
            old_formatting = warnings.formatwarning
            warnings.formatwarning = format_message
            warnings.warn(
                message=message,
                category=exception,
                stacklevel=get_user_code_stack_level(),
            )
            warnings.formatwarning = old_formatting
            warned_once.add(message_hash)


def warn_about_unsupported_type(type_str: str):
    warn_once(
        message=f"""You're attempting to log a type that is not directly supported by Neptune ({type_str}).
        Convert the value to a supported type, such as a string or float, or use stringify_unsupported(obj)
        for dictionaries or collections that contain unsupported values.
        For more, see https://docs.neptune.ai/help/value_of_unsupported_type""",
        exception=NeptuneUnsupportedType,
    )
