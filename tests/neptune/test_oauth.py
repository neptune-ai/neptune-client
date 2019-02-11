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

import time
import unittest

import jwt
from mock import MagicMock, patch
from oauthlib.oauth2 import TokenExpiredError

from neptune.oauth import NeptuneAuth, NeptuneAuthenticator, _no_token_updater
from tests.neptune.http_objects_factory import a_request
from tests.neptune.oauth_objects_factory import SECRET, a_refresh_token, an_access_token


class TestNeptuneAuth(unittest.TestCase):
    def setUp(self):
        super(TestNeptuneAuth, self).setUp()

        self.session = MagicMock()
        self.neptune_auth = NeptuneAuth(self.session)
        self.request = a_request()

        self.url, self.method, self.body, self.headers = \
            self.request.url, self.request.method, self.request.body, self.request.headers

        self.updated_url, self.updated_headers, self.updated_body = \
            a_request().url, a_request().headers, a_request().body

    def test_add_valid_token(self):
        # given
        # pylint: disable=protected-access
        self.session._client.add_token.return_value = self.updated_url, self.updated_headers, self.updated_body

        # when
        updated_request = self.neptune_auth(self.request)

        # then
        # pylint: disable=protected-access
        self.session._client.add_token.assert_called_once_with(
            self.url, http_method=self.method, body=self.body, headers=self.headers)

        # and
        self.assertEqual(self.updated_url, updated_request.url)
        self.assertEqual(self.updated_headers, updated_request.headers)
        self.assertEqual(self.updated_body, updated_request.body)

    def test_refresh_token_and_add(self):
        # given
        # pylint: disable=protected-access
        self.session._client.add_token.side_effect = [
            TokenExpiredError, (self.updated_url, self.updated_headers, self.updated_body)
        ]

        # when
        updated_request = self.neptune_auth(self.request)

        # then
        # pylint: disable=protected-access
        self.session._client.add_token.assert_called_with(
            self.url, http_method=self.method, body=self.body, headers=self.headers)

        # and
        self.assertEqual(self.updated_url, updated_request.url)
        self.assertEqual(self.updated_headers, updated_request.headers)
        self.assertEqual(self.updated_body, updated_request.body)


class TestNeptuneAuthenticator(unittest.TestCase):

    @patch('neptune.oauth.OAuth2Session')
    @patch('neptune.oauth.time')
    def test_apply_oauth2_session_to_request(self, time_mock, session_mock):
        # given
        auth_tokens = MagicMock()
        auth_tokens.accessToken = an_access_token()
        auth_tokens.refreshToken = a_refresh_token()
        decoded_access_token = jwt.decode(auth_tokens.accessToken, SECRET)

        # and
        now = time.time()
        time_mock.time.return_value = now

        # and
        session = MagicMock()
        session_mock.return_value = session

        # and
        neptune_authenticator = NeptuneAuthenticator(auth_tokens)
        request = a_request()

        # when
        updated_request = neptune_authenticator.apply(request)

        # then
        expected_token = {
            'access_token': auth_tokens.accessToken,
            'refresh_token': auth_tokens.refreshToken,
            'expires_in': decoded_access_token['exp'] - now
        }

        expected_auto_refresh_url = '{realm_url}/protocol/openid-connect/token'.format(
            realm_url=decoded_access_token['iss']
        )

        session_mock.assert_called_once_with(client_id=decoded_access_token['azp'],
                                             token=expected_token,
                                             auto_refresh_url=expected_auto_refresh_url,
                                             auto_refresh_kwargs={'client_id': decoded_access_token['azp']},
                                             token_updater=_no_token_updater)

        # and
        self.assertEqual(session, updated_request.auth.session)


if __name__ == '__main__':
    unittest.main()
