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
__all__ = ["init_model"]

from typing import Optional

from neptune.internal.init.parameters import DEFAULT_FLUSH_PERIOD
from neptune.metadata_containers import Model


def init_model(
    with_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    key: Optional[str] = None,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    mode: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
) -> Model:
    return Model(
        with_id=with_id,
        name=name,
        key=key,
        project=project,
        api_token=api_token,
        mode=mode,
        flush_period=flush_period,
        proxies=proxies,
    )
