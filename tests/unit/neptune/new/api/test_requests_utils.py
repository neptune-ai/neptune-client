#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from bravado.requests_client import RequestsResponseAdapter
from requests import Response

from neptune.api.requests_utils import ensure_json_response


class EmptyResponse(Response):
    def __init__(self):
        super().__init__()
        self._content = b""


class InvalidResponse(Response):
    def __init__(self):
        super().__init__()
        self._content = b"deadbeef"


class JsonResponse(Response):
    def __init__(self):
        super().__init__()
        self.headers["Content-Type"] = "application/json"
        self._content = '{"key": "value"}'.encode("utf-8")


def test_ensure_json_body__if_empty():
    # given - response with no json body
    empty_server_response = RequestsResponseAdapter(requests_lib_response=EmptyResponse())

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {}


def test_ensure_json_body__invalid():
    # given - response with no json body
    empty_server_response = RequestsResponseAdapter(requests_lib_response=InvalidResponse())

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {}


def test_ensure_json_body__standard():
    # given - response with no json body
    empty_server_response = RequestsResponseAdapter(requests_lib_response=JsonResponse())

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {"key": "value"}
