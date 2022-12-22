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
__all__ = ["Model"]

from typing import (
    Iterable,
    Optional,
)

from neptune.new.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQueryAggregate,
    NQLQueryAttribute,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.metadata_containers import MetadataContainer
from neptune.new.metadata_containers.metadata_containers_table import Table


class Model(MetadataContainer):
    """A class for managing a Neptune model and retrieving information from it.

    You may also want to check `Model docs page`_.

    .. _Model docs page:
       https://docs.neptune.ai/api/model
    """

    container_type = ContainerType.MODEL

    @property
    def _docs_url_stop(self) -> str:
        return "https://docs.neptune.ai/api/model#stop"

    @property
    def _label(self) -> str:
        return self._sys_id

    @property
    def _url(self) -> str:
        return self._backend.get_model_url(
            model_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
        )

    @property
    def _metadata_url(self) -> str:
        return self._url.rstrip("/") + "/metadata"

    def fetch_model_versions_table(self, columns: Optional[Iterable[str]] = None) -> Table:
        """Retrieve all versions of the given model.

        Args:
            columns: Names of columns to include in the table, as a list of namespace or field names.
                The Neptune ID ("sys/id") is included automatically.
                Examples:
                    Fields: `["params/lr", "params/batch", "val/acc"]` - these fields are included as columns.
                    Namespaces: `["params", "val"]` - all the fields inside the namespaces are included as columns.
                If `None` (default), all the columns of the model versions table are included.

        Returns:
            `Table` object containing `ModelVersion` objects that match the specified criteria.

            Use `to_pandas()` to convert it to a pandas DataFrame.

        Examples:
            >>> import neptune.new as neptune

            >>> # Initialize model with the ID "CLS-FOREST"
            ... model = neptune.init_model(model="CLS-FOREST")

            >>> # Fetch the metadata of all model versions as a pandas DataFrame
            ... model_versions_df = model.fetch_model_versions_table().to_pandas()

            >>> # Fetch the metadata of all model versions as a pandas DataFrame,
            ... # including only the fields "params/lr" and "val/loss" as columns:
            ... model_versions = model.fetch_model_versions_table(columns=["params/lr", "val/loss"])
            ... model_versions_df = model_versions.to_pandas()

            >>> # Sort model versions by size
            ... model_versions_df = model_versions_df.sort_values(by="sys/size")

            >>> # Sort model versions by creation time
            ... model_versions_df = model_versions_df.sort_values(by="sys/creation_time", ascending=False)

            >>> # Extract the last model version ID
            ... last_model_version_id = model_versions_df["sys/id"].values[0]

        You may also want to check the API referene in the docs:
            https://docs.neptune.ai/api/model/#fetch_model_versions_table
        """
        return MetadataContainer._fetch_entries(
            self,
            child_type=ContainerType.MODEL_VERSION,
            query=NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/model_id",
                        value=self._sys_id,
                        operator=NQLAttributeOperator.EQUALS,
                        type=NQLAttributeType.STRING,
                    ),
                    NQLQueryAttribute(
                        name="sys/trashed",
                        type=NQLAttributeType.BOOLEAN,
                        operator=NQLAttributeOperator.EQUALS,
                        value=False,
                    ),
                ],
                aggregator=NQLAggregator.AND,
            ),
            columns=columns,
        )
