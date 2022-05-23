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

import json
from json.decoder import JSONDecodeError
from typing import Optional

from bravado.client import SwaggerClient
from bravado.exception import HTTPError

from neptune.new.exceptions import (
    NeptuneFieldCountLimitExceedException,
    NeptuneLimitExceedException,
)


class ApiMethodWrapper:
    ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED = "ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED"
    WORKSPACE_IN_READ_ONLY_MODE = "WORKSPACE_IN_READ_ONLY_MODE"

    def __init__(self, api_method):
        self._api_method = api_method

    @staticmethod
    def _parse_error_type(response) -> Optional[str]:
        try:
            error_data = json.loads(response.text)
            return error_data.get("errorType") if error_data is not None else None
        except JSONDecodeError:
            return None

    @staticmethod
    def handle_neptune_http_errors(response, exception: Optional[HTTPError] = None):
        error_type: Optional[str] = ApiMethodWrapper._parse_error_type(response)
        if error_type == ApiMethodWrapper.ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED:
            # TODO We need to do something about this "lack of identifiers" in error messages.
            raise NeptuneFieldCountLimitExceedException()
        elif error_type == ApiMethodWrapper.WORKSPACE_IN_READ_ONLY_MODE:
            raise NeptuneLimitExceedException(
                reason=response.json().get("title", "Unknown reason")
            ) from exception
        elif exception:
            raise exception

    def __call__(self, *args, **kwargs):
        try:
            future = self._api_method(*args, **kwargs)
            return FinishedApiResponseFuture(future.response())  # wait synchronously
        except HTTPError as e:
            self.handle_neptune_http_errors(e.response, exception=e)

    def __getattr__(self, item):
        return getattr(self._api_method, item)


class ApiWrapper:
    def __init__(self, api_obj):
        self._api_obj = api_obj

    def __getattr__(self, item):
        return ApiMethodWrapper(getattr(self._api_obj, item))


class FinishedApiResponseFuture:
    def __init__(self, response):
        self._response = response

    def response(self):
        return self._response


class SwaggerClientWrapper:
    def __init__(self, swagger_client: SwaggerClient):
        self._swagger_client = swagger_client
        self.api = ApiWrapper(swagger_client.api)
        self.swagger_spec = swagger_client.swagger_spec

    # For test purposes
    def __eq__(self, other):
        if isinstance(other, SwaggerClientWrapper):
            return self._swagger_client == other._swagger_client
        return False
