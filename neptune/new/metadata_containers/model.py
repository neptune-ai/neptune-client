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
from neptune.new.metadata_containers import MetadataContainer
from neptune.new.internal.container_type import ContainerType
from neptune.new.metadata_containers.metadata_containers_table import Table
from neptune.new.internal.backends.nql import (
    NQLQueryAttribute,
    NQLAttributeOperator,
    NQLAttributeType,
)


class Model(MetadataContainer):
    """A class for managing a Neptune model and retrieving information from it.

    You may also want to check `Model docs page`_.

    .. _Model docs page:
       https://docs.neptune.ai/api-reference/model
    """

    container_type = ContainerType.MODEL

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api-reference/model#.stop"

    @property
    def _label(self) -> str:
        return self._sys_id

    def fetch_model_versions_table(self) -> Table:
        """TODO: NPT-11349"""
        return MetadataContainer._fetch_entries(
            self,
            type=ContainerType.MODEL_VERSION,
            query=NQLQueryAttribute(
                name="sys/model_id",
                value=self._sys_id,
                operator=NQLAttributeOperator.EQUALS,
                type=NQLAttributeType.STRING,
            ),
        )
