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

from neptune.common.deprecation import warn_once
from neptune.new.exceptions import NeptuneParametersCollision

__all__ = ["deprecated", "deprecated_parameter"]


def deprecated(*, alternative: Optional[str] = None):
    def deco(func):
        @wraps(func)
        def inner(*args, **kwargs):
            additional_info = f", use `{alternative}` instead" if alternative else " and will be removed"

            warn_once(
                message=f"`{func.__name__}` is deprecated{additional_info}."
                f" We'll end support of it in `neptune-client==1.0.0`."
                " For details, see https://docs.neptune.ai/setup/neptune-client_1-0_release_changes"
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
                    " We'll end support of it in `neptune-client==1.0.0`."
                )

                kwargs[required_kwarg_name] = kwargs[deprecated_kwarg_name]
                del kwargs[deprecated_kwarg_name]

            return f(*args, **kwargs)

        return inner

    return deco
