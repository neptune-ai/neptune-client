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
from functools import wraps
from typing import Optional

from neptune.new.exceptions import NeptuneParametersCollision
from neptune.new.internal.utils.logger import logger

__all__ = ["deprecated", "deprecated_parameter"]


def deprecated(*, alternative: Optional[str] = None):
    def deco(func):
        @wraps(func)
        def inner(*args, **kwargs):
            if alternative:
                logger.warning(
                    "Function `%s` is deprecated, use `%s` instead."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    func.__name__,
                    alternative,
                )
            else:
                logger.warning(
                    "Function `%s` is deprecated and will be removed."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    func.__name__,
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

                logger.warning(
                    "Parameter `%s` is deprecated, use `%s` instead."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    deprecated_kwarg_name,
                    required_kwarg_name,
                )

                kwargs[required_kwarg_name] = kwargs[deprecated_kwarg_name]
                del kwargs[deprecated_kwarg_name]

            return f(*args, **kwargs)

        return inner

    return deco
