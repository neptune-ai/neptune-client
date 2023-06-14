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
from typing import List

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper


@with_api_exceptions_handler
def trash_experiments(*, client: SwaggerClientWrapper, project_identifier: str, batch_ids: List[str]):
    params = {
        "projectIdentifier": project_identifier,
        "experimentIdentifiers": batch_ids,
        **DEFAULT_REQUEST_KWARGS,
    }
    return client.api.trashExperiments(**params).response()
