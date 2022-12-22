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
__all__ = ["Credentials"]

import base64
import json
import os
from dataclasses import dataclass
from typing import (
    Dict,
    Optional,
)

from neptune.common.envs import API_TOKEN_ENV_NAME
from neptune.common.exceptions import NeptuneInvalidApiTokenException
from neptune.new import (
    ANONYMOUS,
    ANONYMOUS_API_TOKEN,
)
from neptune.new.exceptions import NeptuneMissingApiTokenException


@dataclass(frozen=True)
class Credentials:
    api_token: str
    token_origin_address: str
    api_url_opt: str

    @classmethod
    def from_token(cls, api_token: Optional[str] = None) -> "Credentials":
        if api_token is None:
            api_token = os.getenv(API_TOKEN_ENV_NAME)

        if api_token == ANONYMOUS:
            api_token = ANONYMOUS_API_TOKEN

        if api_token is None:
            raise NeptuneMissingApiTokenException()

        api_token = api_token.strip()
        token_dict = Credentials._api_token_to_dict(api_token)
        # TODO: Consider renaming 'api_address' (breaking backward compatibility)
        if "api_address" not in token_dict:
            raise NeptuneInvalidApiTokenException()
        token_origin_address = token_dict["api_address"]
        api_url = token_dict["api_url"] if "api_url" in token_dict else None

        return Credentials(
            api_token=api_token,
            token_origin_address=token_origin_address,
            api_url_opt=api_url,
        )

    @staticmethod
    def _api_token_to_dict(api_token: str) -> Dict[str, str]:
        try:
            return json.loads(base64.b64decode(api_token.encode()).decode("utf-8"))
        except Exception:
            raise NeptuneInvalidApiTokenException()
