#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from neptune.new.exceptions import NeptuneParametersCollision
from neptune.new.internal.utils.logger import logger


def deprecated_parameter(*, deprecated_kwarg_name, required_kwarg_name):
    def deco(f):
        @wraps(f)
        def inner(*args, **kwargs):
            deprecated_param = kwargs.get(deprecated_kwarg_name)
            required_param = kwargs.get(required_kwarg_name)
            if deprecated_param is not None:
                if required_param is not None:
                    raise NeptuneParametersCollision(
                        required_kwarg_name, deprecated_kwarg_name, method_name=f.__name__
                    )

                logger.warning(
                    "parameter `{deprecated_kwarg_name}` is deprecated, use `{required_kwarg_name}` instead."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    extra={
                        "deprecated_kwarg_name": deprecated_kwarg_name,
                        "required_kwarg_name": required_kwarg_name,
                    },
                )

                kwargs[required_kwarg_name] = deprecated_param
                del kwargs[deprecated_kwarg_name]

            return f(*args, **kwargs)

        return inner

    return deco
