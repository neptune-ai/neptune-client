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

import uuid
from typing import List

from neptune.new.exceptions import OfflineModeFetchException
from neptune.new.internal.backends.api_model import Attribute, FloatAttribute, StringAttribute, \
    DatetimeAttribute, FloatSeriesAttribute, StringSeriesAttribute, StringSetAttribute
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock


class OfflineNeptuneBackend(NeptuneBackendMock):

    def get_attributes(self, experiment_uuid: uuid.UUID) -> List[Attribute]:
        raise OfflineModeFetchException

    def get_float_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> FloatAttribute:
        raise OfflineModeFetchException

    def get_string_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringAttribute:
        raise OfflineModeFetchException

    def get_datetime_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> DatetimeAttribute:
        raise OfflineModeFetchException

    def get_float_series_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> FloatSeriesAttribute:
        raise OfflineModeFetchException

    def get_string_series_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringSeriesAttribute:
        raise OfflineModeFetchException

    def get_string_set_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringSetAttribute:
        raise OfflineModeFetchException
