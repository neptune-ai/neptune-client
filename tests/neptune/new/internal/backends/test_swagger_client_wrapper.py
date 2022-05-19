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
import unittest
from unittest.mock import MagicMock

from neptune.new.internal.backends.swagger_client_wrapper import SwaggerClientWrapper


class TestSwaggerClientWrapper(unittest.TestCase):
    # pylint:disable=protected-access

    def setUp(self) -> None:
        pass

    def test_api_callable_objects(self):
        # given
        swagger_client = MagicMock()
        api = MagicMock()
        api.callable_object = MagicMock()
        api.callable_object.sub_property = 13
        swagger_client.api = api

        wrapper = SwaggerClientWrapper(swagger_client)

        # when
        wrapper.api.method("arg1", kwarg="kwarg1")
        wrapper.api.callable_object("arg2", kwarg="kwarg2")

        # then
        api.method.assert_called_once_with("arg1", kwarg="kwarg1")
        api.callable_object.assert_called_once_with("arg2", kwarg="kwarg2")
        self.assertEqual(13, wrapper.api.callable_object.sub_property)
