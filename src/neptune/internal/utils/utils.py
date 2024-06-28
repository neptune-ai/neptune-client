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
import ssl
import sys

from neptune.internal.utils.logger import get_logger

_logger = get_logger()

IS_WINDOWS = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"


def reset_internal_ssl_state():
    """
    OpenSSL's internal random number generator does not properly handle forked processes.
    Applications must change the PRNG state of the parent process if they use any SSL feature with os.fork().
    Any successful call of RAND_add(), RAND_bytes() or RAND_pseudo_bytes() is sufficient.
    https://docs.python.org/3/library/ssl.html#multi-processing
    """
    ssl.RAND_bytes(100)


def update_session_proxies(session, proxies):
    if proxies is not None:
        try:
            session.proxies.update(proxies)
        except (TypeError, ValueError):
            raise ValueError("Wrong proxies format: {}".format(proxies))


def is_ipython():
    try:
        import IPython

        ipython = IPython.core.getipython.get_ipython()
        return ipython is not None
    except ImportError:
        return False
