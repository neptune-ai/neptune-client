#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

__all__ = ["HostedNeptuneBackendV2"]


from neptune_api.api.backend import get_project
from neptune_api.credentials import Credentials
from neptune_api.models import ProjectDTO

from neptune.internal.backends.api_client import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.id_formats import QualifiedName


class HostedNeptuneBackendV2(NeptuneBackend):
    def __init__(self, credentials: Credentials) -> None:
        self.credentials = credentials

        config, token_urls = get_config_and_token_urls(self.credentials)
        self.auth_client = create_auth_api_client(credentials, config, token_urls)

    # only happy path is implemented
    def get_project(self, project_identifier: QualifiedName) -> ProjectDTO:
        response = get_project.sync_detailed(client=self.auth_client, project_identifier=project_identifier)
        return response.parsed
