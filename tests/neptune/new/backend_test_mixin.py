#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from mock import MagicMock


class BackendTestMixin:

    @staticmethod
    def _get_swagger_client_mock(
            swagger_client_factory,
            min_recommended=None,
            min_compatible=None,
            max_compatible=None):
        py_lib_versions = type('py_lib_versions', (object,), {})()
        setattr(py_lib_versions, "minRecommendedVersion", min_recommended)
        setattr(py_lib_versions, "minCompatibleVersion", min_compatible)
        setattr(py_lib_versions, "maxCompatibleVersion", max_compatible)

        client_config = type('client_config_response_result', (object,), {})()
        setattr(client_config, "pyLibVersions", py_lib_versions)
        setattr(client_config, "apiUrl", "ui.neptune.ai")
        setattr(client_config, "applicationUrl", "ui.neptune.ai")

        swagger_client = MagicMock()
        swagger_client.api.getClientConfig.return_value.response.return_value.result = client_config
        swagger_client_factory.return_value = swagger_client

        return swagger_client
