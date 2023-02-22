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

from neptune.common.warnings import warn_once


def legacy_client_deprecation(func):
    @wraps(func)
    def inner(*args, **kwargs):
        warn_once(
            message="You're using a legacy version of Neptune client."
            " It will be moved to `neptune.legacy` as of `neptune-client==1.0.0`."
        )
        return func(*args, **kwargs)

    return inner
