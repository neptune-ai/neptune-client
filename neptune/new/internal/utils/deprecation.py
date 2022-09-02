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

from neptune.new.exceptions import NeptuneInitParametersCollision
from neptune.new.internal.utils.logger import logger


def deprecated_id_parameter(*, deprecated_kwarg_name):
    def deco(f):
        @wraps(f)
        def inner(*args, **kwargs):
            deprecated_param = kwargs.get(deprecated_kwarg_name)
            with_id = kwargs.get("with_id")
            if deprecated_param is not None:
                if with_id is not None:
                    raise NeptuneInitParametersCollision(
                        "with_id", deprecated_kwarg_name, method_name=f.__name__
                    )

                logger.warning(
                    "parameter `{deprecated_kwarg_name}` is deprecated, use `with_id` instead."
                    " We'll end support of it in `neptune-client==1.0.0`.",
                    deprecated_kwarg_name=deprecated_kwarg_name,
                )

                kwargs["with_id"] = deprecated_param
                del kwargs[deprecated_kwarg_name]

            return f(*args, **kwargs)

        return inner

    return deco
