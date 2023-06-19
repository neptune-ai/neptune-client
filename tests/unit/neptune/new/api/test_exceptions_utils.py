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
from pytest import raises

from neptune.api.exceptions_utils import handle_json_errors


def test_handle_json_errors__empty():
    # given
    content = {}
    error_processors = {}

    # then
    with raises(ValueError):
        handle_json_errors(
            content=content, source_exception=ValueError("source exception"), error_processors=error_processors
        )


def test_handle_json_errors__supported():
    # given
    content = {"errorType": "NOT_IMPLEMENTED_EXCEPTION"}
    error_processors = {"NOT_IMPLEMENTED_EXCEPTION": lambda _: NotImplementedError()}

    # then
    with raises(NotImplementedError):
        handle_json_errors(
            content=content, source_exception=ValueError("source exception"), error_processors=error_processors
        )


def test_handle_json_errors__not_supported():
    # given
    content = {"errorType": "SOME_ANOTHER_EXCEPTION"}
    error_processors = {"NOT_IMPLEMENTED": lambda _: NotImplementedError()}

    # then
    with raises(ValueError):
        handle_json_errors(
            content=content, source_exception=ValueError("source exception"), error_processors=error_processors
        )


def test_handle_json_errors__default():
    # given
    content = {"errorType": "SOME_ANOTHER_EXCEPTION"}
    error_processors = {"NOT_IMPLEMENTED": lambda _: NotImplementedError()}

    # then
    with raises(IndexError):
        handle_json_errors(
            content=content,
            source_exception=ValueError("source exception"),
            error_processors=error_processors,
            default_exception=IndexError("default exception"),
        )


def test_handle_json_errors__access_to_content():
    # given
    content = {"errorType": "NOT_IMPLEMENTED", "message": "some message"}
    error_processors = {"NOT_IMPLEMENTED": lambda data: NotImplementedError(data.get("message"))}

    # then
    with raises(NotImplementedError) as exception_info:
        handle_json_errors(
            content=content,
            source_exception=ValueError("source exception"),
            error_processors=error_processors,
            default_exception=IndexError("default exception"),
        )

    assert str(exception_info.value) == "some message"
