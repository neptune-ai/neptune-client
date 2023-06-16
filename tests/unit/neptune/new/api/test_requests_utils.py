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


class TestResponse(Response):
    def __init__(self, content: bytes) -> None:
        super().__init__()
        self._content = content


def test_ensure_json_body__if_empty():
    # given
    empty_server_response = RequestsResponseAdapter(TestResponse(content=b""))

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {}


def test_ensure_json_body__invalid():
    # given
    empty_server_response = RequestsResponseAdapter(TestResponse(content=b"deadbeef"))

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {}


def test_ensure_json_body__standard():
    # given
    empty_server_response = RequestsResponseAdapter(TestResponse(content='{"key": "value"}'.encode("utf-8")))

    # when
    body = ensure_json_response(empty_server_response)

    # then
    assert body == {"key": "value"}
