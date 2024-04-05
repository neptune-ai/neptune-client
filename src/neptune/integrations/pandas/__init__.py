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
from __future__ import annotations

__all__ = ["to_pandas"]

from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Dict,
    Optional,
    Tuple,
    Union,
)

import pandas as pd

from neptune.api.models import (
    ArtifactField,
    BoolField,
    DateTimeField,
    FieldVisitor,
    FileField,
    FileSetField,
    FloatField,
    FloatSeriesField,
    GitRefField,
    ImageSeriesField,
    IntField,
    LeaderboardEntry,
    NotebookRefField,
    ObjectStateField,
    StringField,
    StringSeriesField,
    StringSetField,
)

if TYPE_CHECKING:
    from neptune.table import Table

PANDAS_AVAILABLE_TYPES = Union[str, float, int, bool, datetime, None]


class FieldToPandasValueVisitor(FieldVisitor[PANDAS_AVAILABLE_TYPES]):

    def visit_float(self, field: FloatField) -> float:
        return field.value

    def visit_int(self, field: IntField) -> int:
        return field.value

    def visit_bool(self, field: BoolField) -> bool:
        return field.value

    def visit_string(self, field: StringField) -> str:
        return field.value

    def visit_datetime(self, field: DateTimeField) -> datetime:
        return field.value

    def visit_file(self, field: FileField) -> None:
        return None

    def visit_string_set(self, field: StringSetField) -> Optional[str]:
        return ",".join(field.values)

    def visit_float_series(self, field: FloatSeriesField) -> Optional[float]:
        return field.last

    def visit_string_series(self, field: StringSeriesField) -> Optional[str]:
        return field.last

    def visit_image_series(self, field: ImageSeriesField) -> None:
        return None

    def visit_file_set(self, field: FileSetField) -> None:
        return None

    def visit_git_ref(self, field: GitRefField) -> Optional[str]:
        return field.commit.commit_id if field.commit is not None else None

    def visit_object_state(self, field: ObjectStateField) -> str:
        return field.value

    def visit_notebook_ref(self, field: NotebookRefField) -> Optional[str]:
        return field.notebook_name

    def visit_artifact(self, field: ArtifactField) -> str:
        return field.hash


def make_row(entry: LeaderboardEntry, to_value_visitor: FieldVisitor) -> Dict[str, PANDAS_AVAILABLE_TYPES]:
    row: Dict[str, PANDAS_AVAILABLE_TYPES] = dict()

    for field in entry.fields:
        value = to_value_visitor.visit(field)
        if value is not None:
            row[field.path] = value

    return row


def sort_key(field: str) -> Tuple[int, str]:
    domain = field.split("/")[0]
    if domain == "sys":
        return 0, field
    if domain == "monitoring":
        return 2, field
    return 1, field


def to_pandas(table: Table) -> pd.DataFrame:

    to_value_visitor = FieldToPandasValueVisitor()
    rows = dict((n, make_row(entry, to_value_visitor)) for (n, entry) in enumerate(table._entries))

    df = pd.DataFrame.from_dict(data=rows, orient="index")
    df = df.reindex(sorted(df.columns, key=sort_key), axis="columns")

    return df
