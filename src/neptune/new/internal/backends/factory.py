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
__all__ = ["get_backend"]

from typing import Optional

from neptune.new.internal.credentials import Credentials
from neptune.new.types.mode import Mode

from .hosted_neptune_backend import HostedNeptuneBackend
from .neptune_backend import NeptuneBackend
from .neptune_backend_mock import NeptuneBackendMock
from .offline_neptune_backend import OfflineNeptuneBackend


def get_backend(mode: Mode, api_token: Optional[str] = None, proxies: Optional[dict] = None) -> NeptuneBackend:
    if mode == Mode.ASYNC:
        return HostedNeptuneBackend(credentials=Credentials.from_token(api_token=api_token), proxies=proxies)
    elif mode == Mode.SYNC:
        return HostedNeptuneBackend(credentials=Credentials.from_token(api_token=api_token), proxies=proxies)
    elif mode == Mode.DEBUG:
        return NeptuneBackendMock()
    elif mode == Mode.OFFLINE:
        return OfflineNeptuneBackend()
    elif mode == Mode.READ_ONLY:
        return HostedNeptuneBackend(credentials=Credentials.from_token(api_token=api_token), proxies=proxies)
    else:
        raise ValueError(f"mode should be one of {[m for m in Mode]}")
