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

import jwt
from bravado.requests_client import Authenticator
from oauthlib.oauth2 import TokenExpiredError
from requests.auth import AuthBase
from requests_oauthlib import OAuth2Session


class NeptuneAuth(AuthBase):

    def __init__(self, session):
        self.session = session

    def __call__(self, r):
        try:
            return self._add_token(r)
        except TokenExpiredError:
            self.session.refresh_token(self.session.auto_refresh_url)
            return self._add_token(r)

    def _add_token(self, r):
        # pylint: disable=protected-access
        r.url, r.headers, r.body = self.session._client.add_token(r.url,
                                                                  http_method=r.method,
                                                                  body=r.body,
                                                                  headers=r.headers)
        return r


class NeptuneAuthenticator(Authenticator):

    def __init__(self, auth_tokens):
        super(NeptuneAuthenticator, self).__init__(host='')
        decoded_json_token = jwt.decode(auth_tokens.accessToken, verify=False)
        expires_at = decoded_json_token.get(u'exp')
        client_name = decoded_json_token.get(u'azp')
        refresh_url = u'{realm_url}/protocol/openid-connect/token'.format(realm_url=decoded_json_token.get(u'iss'))
        token = {
            u'access_token': auth_tokens.accessToken,
            u'refresh_token': auth_tokens.refreshToken,
            u'expires_in': expires_at - time.time()
        }
        self.auth = NeptuneAuth(
            OAuth2Session(
                client_id=client_name,
                token=token,
                auto_refresh_url=refresh_url,
                auto_refresh_kwargs={'client_id': client_name},
                token_updater=_no_token_updater))

    def matches(self, url):
        return True

    def apply(self, request):
        request.auth = self.auth
        return request


def _no_token_updater():
    # For unit tests.
    return None
