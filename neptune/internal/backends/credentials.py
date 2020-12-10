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
import logging
import os

from neptune.constants import ANONYMOUS, ANONYMOUS_API_TOKEN

from neptune import envs
from neptune.api_exceptions import InvalidApiKey
from neptune.exceptions import NeptuneMissingApiTokenException

_logger = logging.getLogger(__name__)


class Credentials(object):
    """It formats your Neptune api token to the format that can be understood by the Neptune Client.

    A constructor allowing you to pass the Neptune API token.

    Args:
        api_token(str): This is a secret API key that you can retrieve by running
            `$ neptune account api-token get`

    Attributes:
        api_token:  This is a secret API key that was passed at instantiation.

    Examples:

        >>> from neptune.internal.backends.credentials import Credentials
        >>> credentials=Credentials('YOUR_NEPTUNE_API_KEY')

        Alternatively you can create an environment variable by running:

        $ export NEPTUNE_API_TOKEN=YOUR_API_TOKEN

        which will allow you to use the same method without `api_token` parameter provided.

        >>> credentials=Credentials()

    Note:
        For security reasons it is recommended to provide api_token through environment variable `NEPTUNE_API_TOKEN`.
        You can do that by going to your console and running:

        $ export NEPTUNE_API_TOKEN=YOUR_API_TOKEN`

        Token provided through environment variable takes precedence over `api_token` parameter.
    """

    def __init__(self, api_token=None):
        if api_token is None:
            api_token = os.getenv(envs.API_TOKEN_ENV_NAME)

        if api_token == ANONYMOUS:
            api_token = ANONYMOUS_API_TOKEN

        self._api_token = api_token
        if self.api_token is None:
            raise NeptuneMissingApiTokenException()

        token_dict = self._api_token_to_dict(self.api_token)
        self._token_origin_address = token_dict['api_address']
        self._api_url = token_dict['api_url'] if 'api_url' in token_dict else None

    @property
    def api_token(self):
        return self._api_token

    @property
    def token_origin_address(self):
        return self._token_origin_address

    @property
    def api_url_opt(self):
        return self._api_url

    @staticmethod
    def _api_token_to_dict(api_token):
        try:
            return json.loads(base64.b64decode(api_token.encode()).decode("utf-8"))
        except Exception:
            raise InvalidApiKey()
