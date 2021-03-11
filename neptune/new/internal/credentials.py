#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

import base64
import json
import os
from typing import Optional, Dict

from neptune.new import envs
from neptune.new import ANONYMOUS, ANONYMOUS_API_TOKEN
from neptune.new.exceptions import NeptuneInvalidApiTokenException, NeptuneMissingApiTokenException


class Credentials(object):

    def __init__(self, api_token: Optional[str] = None):
        if api_token is None:
            api_token = os.getenv(envs.API_TOKEN_ENV_NAME)

        if api_token == ANONYMOUS:
            api_token = ANONYMOUS_API_TOKEN

        self._api_token = api_token
        if self.api_token is None:
            raise NeptuneMissingApiTokenException()

        token_dict = self._api_token_to_dict(self.api_token)
        # TODO: Consider renaming 'api_address' (breaking backward compatibility)
        if 'api_address' not in token_dict:
            raise NeptuneInvalidApiTokenException()
        self._token_origin_address = token_dict['api_address']
        self._api_url = token_dict['api_url'] if 'api_url' in token_dict else None

    @property
    def api_token(self) -> str:
        return self._api_token

    @property
    def token_origin_address(self) -> str:
        return self._token_origin_address

    @property
    def api_url_opt(self) -> Optional[str]:
        return self._api_url

    @staticmethod
    def _api_token_to_dict(api_token: str) -> Dict[str, str]:
        try:
            return json.loads(base64.b64decode(api_token.encode()).decode("utf-8"))
        except Exception:
            raise NeptuneInvalidApiTokenException()
