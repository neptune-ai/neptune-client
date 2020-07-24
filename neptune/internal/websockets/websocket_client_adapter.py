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

import os
import ssl

from six.moves import urllib

from future.utils import PY3
from websocket import ABNF, create_connection


class WebsocketClientAdapter(object):
    def __init__(self):
        self._ws_client = None

    def connect(self, url, token, proxies=None):
        sslopt = None
        if os.getenv("NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"):
            sslopt = {"cert_reqs": ssl.CERT_NONE}

        proto = url.split(':')[0].replace('ws', 'http')
        proxy = proxies[proto] if proxies and proto in proxies else os.getenv("{}_PROXY".format(proto.upper()))

        if proxy:
            proxy_split = urllib.parse.urlparse(proxy).netloc.split(':')
            proxy_host = proxy_split[0]
            proxy_port = proxy_split[1] if len(proxy_split) > 1 else "80" if proto == "http" else "443"
        else:
            proxy_host = None
            proxy_port = None

        self._ws_client = create_connection(url, header=self._auth_header(token), sslopt=sslopt,
                                            http_proxy_host=proxy_host, http_proxy_port=proxy_port)

    def recv(self):
        if self._ws_client is None:
            raise WebsocketNotConnectedException()

        opcode, data = None, None

        while opcode != ABNF.OPCODE_TEXT:
            opcode, data = self._ws_client.recv_data()

        return data.decode("utf-8") if PY3 else data

    @property
    def connected(self):
        return self._ws_client and self._ws_client.connected

    def close(self):
        if self._ws_client:
            return self._ws_client.close()

    def abort(self):
        if self._ws_client:
            return self._ws_client.abort()

    def shutdown(self):
        if self._ws_client:
            return self._ws_client.shutdown()

    @classmethod
    def _auth_header(cls, token):
        return ["Authorization: Bearer " + token['access_token']]


class WebsocketNotConnectedException(Exception):
    def __init__(self):
        super(WebsocketNotConnectedException, self).__init__('Websocket client is not connected!')
