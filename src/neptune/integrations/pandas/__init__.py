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
__all__ = [
    'to_pandas'
]

from typing import (
    Tuple,
    Dict,
    Optional,
    Union,
)
from datetime import datetime

from neptune.metadata_containers.tables import (
    StringSet,
    File,
    FileSet,
    ImageSeries,
    ToValueVisitor, Table, TableEntry,
)


try:
    import pandas as pd
except ModuleNotFoundError as e:
    if e.name == "neptune_optuna":
        from neptune.exceptions import NeptuneIntegrationNotInstalledException, MetadataInconsistency

        raise NeptuneIntegrationNotInstalledException(
            integration_package_name="pandas", framework_name="pandas"
        ) from None
    else:
        raise


class ToPandasValueVisitor(ToValueVisitor):
    def visit_string_set(self, field: StringSet) -> str:
        return ",".join(list(field.values))

    def visit_file(self, field: File) -> None:
        return None

    def visit_file_set(self, field: FileSet) -> None:
        return None

    def visit_image_series(self, field: ImageSeries) -> None:
        return None


def make_row(entry: TableEntry, ) -> Dict[str, Optional[Union[str, float, datetime]]]:
    to_value_visitor = ToPandasValueVisitor()
    row: Dict[str, Optional[Union[str, float, datetime]]] = dict()

    for field in entry.fields:
        value = to_value_visitor.visit(field)
        if value is not None:
            row[field.path] = value

    return row


def sort_key(attr: str) -> Tuple[int, str]:
    domain = attr.split("/")[0]
    if domain == "sys":
        return 0, attr
    if domain == "monitoring":
        return 2, attr
    return 1, attr


def to_pandas(table: Table) -> pd.DataFrame:
    rows = dict((n, make_row(entry)) for (n, entry) in enumerate(table))
    df = pd.DataFrame.from_dict(data=rows, orient="index")
    df = df.reindex(sorted(df.columns, key=sort_key), axis="columns")
    return df
