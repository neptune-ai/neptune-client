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
__all__ = ["ApiMethodWrapper", "SwaggerClientWrapper"]

from typing import Optional

from bravado.client import SwaggerClient
from bravado.exception import HTTPError

from neptune.new.exceptions import (
    NeptuneFieldCountLimitExceedException,
    NeptuneLimitExceedException,
)


class ApiMethodWrapper:
    ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED = "ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED"
    INCORRECT_IDENTIFIER = "INCORRECT_IDENTIFIER"
    WORKSPACE_IN_READ_ONLY_MODE = "WORKSPACE_IN_READ_ONLY_MODE"
    PROJECT_KEY_COLLISION = "PROJECT_KEY_COLLISION"
    PROJECT_NAME_COLLISION = "PROJECT_NAME_COLLISION"
    PROJECT_KEY_INVALID = "PROJECT_KEY_INVALID"
    PROJECT_NAME_INVALID = "PROJECT_NAME_INVALID"
    EXPERIMENT_NOT_FOUND = "EXPERIMENT_NOT_FOUND"

    def __init__(self, api_method):
        self._api_method = api_method

    @staticmethod
    def handle_neptune_http_errors(response, exception: Optional[HTTPError] = None):
        try:
            body = response.json() or dict()
        except Exception:
            body = {}

        error_type: Optional[str] = body.get("errorType")
        if error_type == ApiMethodWrapper.ATTRIBUTES_PER_EXPERIMENT_LIMIT_EXCEEDED:
            raise NeptuneFieldCountLimitExceedException(
                limit=body.get("limit", "<unknown limit>"),
                container_type=body.get("experimentType", "object"),
                identifier=body.get("experimentQualifiedName", "<unknown identifier>"),
            )
        elif error_type == ApiMethodWrapper.WORKSPACE_IN_READ_ONLY_MODE:
            raise NeptuneLimitExceedException(reason=body.get("title", "Unknown reason")) from exception
        elif error_type == ApiMethodWrapper.INCORRECT_IDENTIFIER:
            from neptune.management.exceptions import IncorrectIdentifierException

            identifier = body.get("identifier", "<Unknown identifier>")
            raise IncorrectIdentifierException(identifier=identifier) from exception
        elif error_type == ApiMethodWrapper.PROJECT_KEY_COLLISION:
            from neptune.management.exceptions import ProjectKeyCollision

            raise ProjectKeyCollision(key=body.get("key", "<unknown key>")) from exception
        elif error_type == ApiMethodWrapper.PROJECT_NAME_COLLISION:
            from neptune.management.exceptions import ProjectNameCollision

            raise ProjectNameCollision(name=body.get("name", "<unknown name>")) from exception
        elif error_type == ApiMethodWrapper.PROJECT_KEY_INVALID:
            from neptune.management.exceptions import ProjectKeyInvalid

            raise ProjectKeyInvalid(
                key=body.get("key", "<unknown key>"), reason=body.get("reason", "Unknown reason")
            ) from exception
        elif error_type == ApiMethodWrapper.PROJECT_NAME_INVALID:
            from neptune.management.exceptions import ProjectNameInvalid

            raise ProjectNameInvalid(
                name=body.get("name", "<unknown name>"), reason=body.get("reason", "Unknown reason")
            ) from exception
        elif error_type == ApiMethodWrapper.EXPERIMENT_NOT_FOUND:
            from neptune.management.exceptions import ObjectNotFound

            raise ObjectNotFound() from exception
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
