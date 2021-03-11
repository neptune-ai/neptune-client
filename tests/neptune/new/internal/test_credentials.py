#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import os
import unittest

from neptune.new.envs import API_TOKEN_ENV_NAME
from neptune.new.exceptions import NeptuneInvalidApiTokenException
from neptune.new.internal.credentials import Credentials

API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ' \
            'hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0='


class TestCredentials(unittest.TestCase):

    def test_should_take_default_credentials_from_env(self):
        # given
        os.environ[API_TOKEN_ENV_NAME] = API_TOKEN

        # when
        credentials = Credentials()

        # then
        self.assertEqual(API_TOKEN, credentials.api_token)

    def test_should_replace_token_from_env(self):
        # given
        os.environ[API_TOKEN_ENV_NAME] = "INVALID_TOKEN"

        # when
        credentials = Credentials(API_TOKEN)

        # then
        self.assertEqual(API_TOKEN, credentials.api_token)

    def test_raise_invalid_token(self):
        # expect
        with self.assertRaises(NeptuneInvalidApiTokenException):
            Credentials("INVALID_TOKEN")

    def test_raise_invalid_token_from_env(self):
        # given
        os.environ[API_TOKEN_ENV_NAME] = "INVALID_TOKEN"

        # expect
        with self.assertRaises(NeptuneInvalidApiTokenException):
            Credentials()
