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

from neptune import envs
from neptune.api_exceptions import InvalidApiKey
from neptune.exceptions import MissingApiToken

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

        >>> from neptune.credentials import Credentials
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
        else:
            _logger.warning(
                "WARNING: It is not secure to place API token in your source code. "
                "You should treat it as a password to your account. "
                "It is strongly recommended to use %s environment variable instead. "
                "Remember not to upload source file with API token to any public repository.",
                envs.API_TOKEN_ENV_NAME)

        self.api_token = api_token
        if self.api_token is None:
            raise MissingApiToken()

    @property
    def api_address(self):
        """ The address of the Neptune API associated with the credentials.

        Returns:
            str: URL address of the Neptune API associated with the credentials.

        Examples:

            >>> from neptune.credentials import Credentials
            >>> credentials=Credentials()
            >>> credentials.api_address
            'https://app.neptune.ml'
        """
        return self._api_token_to_dict(self.api_token)['api_address']

    @staticmethod
    def _api_token_to_dict(api_token):
        try:
            return json.loads(base64.b64decode(api_token.encode()).decode("utf-8"))
        except Exception:
            raise InvalidApiKey()
