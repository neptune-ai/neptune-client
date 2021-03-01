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


class ClientConfig(object):

    def __init__(self, api_url, display_url, min_recommended_version, min_compatible_version, max_compatible_version):
        self._api_url = api_url
        self._display_url = display_url
        self._min_recommended_version = min_recommended_version
        self._min_compatible_version = min_compatible_version
        self._max_compatible_version = max_compatible_version

    @property
    def api_url(self):
        return self._api_url

    @property
    def display_url(self):
        return self._display_url

    @property
    def min_recommended_version(self):
        return self._min_recommended_version

    @property
    def min_compatible_version(self):
        return self._min_compatible_version

    @property
    def max_compatible_version(self):
        return self._max_compatible_version
