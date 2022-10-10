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
import warnings
from functools import wraps
from typing import Optional

from neptune.new.exceptions import NeptuneParametersCollision

__all__ = ["deprecated", "deprecated_parameter", "NeptuneDeprecationWarning", "warn_once"]


class NeptuneDeprecationWarning(DeprecationWarning):
    pass


warnings.simplefilter("always", category=NeptuneDeprecationWarning)


warned_once = set()


def warn_once(message: str, stack_level: int = 1):
    if message not in warned_once:
        warnings.warn(
            message=message,
            category=NeptuneDeprecationWarning,
            stacklevel=stack_level + 1,
        )
        warned_once.add(message)


def deprecated(*, alternative: Optional[str] = None, stack_level: int = 1):
    def deco(func):
        @wraps(func)
        def inner(*args, **kwargs):
            additional_info = f", use `{alternative}` instead" if alternative else " and will be removed"

            warn_once(
                message=f"`{func.__name__}` is deprecated{additional_info}."
                f" We'll end support of it in `neptune-client==1.0.0`.",
                stack_level=stack_level + 1,
            )

            return func(*args, **kwargs)

        return inner

    return deco

__all__ = ["deprecated", "deprecated_parameter"]

def warn_once(message: str, stack_level: int = 1):
    if message not in warned_once.keys():
        warnings.warn(
            message=message,
            category=NeptuneDeprecationWarning,
            stacklevel=stack_level + 1,
        )
        warned_once[message] = 1


def deprecated(*, alternative: Optional[str] = None, stack_level: int = 1):
    def deco(func):
        @wraps(func)
        def inner(*args, **kwargs):
            additional_info = f", use `{alternative}` instead" if alternative else " and will be removed"

            warn_once(
                message=f"`{func.__name__}` is deprecated{additional_info}."
                        f" We'll end support of it in `neptune-client==1.0.0`.",
                stack_level=stack_level + 1,
            )

            return func(*args, **kwargs)

        return inner

    return deco


def deprecated_parameter(*, deprecated_kwarg_name, required_kwarg_name):
    def deco(f):
        @wraps(f)
        def inner(*args, **kwargs):
            if deprecated_kwarg_name in kwargs:
                if required_kwarg_name in kwargs:
                    raise NeptuneParametersCollision(required_kwarg_name, deprecated_kwarg_name, method_name=f.__name__)

                warn_once(
                    message=f"Parameter `{deprecated_kwarg_name}` is deprecated, use `{required_kwarg_name}` instead."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    stack_level=2,
                )
                warnings.simplefilter("default", DeprecationWarning)

                kwargs[required_kwarg_name] = kwargs[deprecated_kwarg_name]
                del kwargs[deprecated_kwarg_name]

            return f(*args, **kwargs)

        return inner

    return deco
