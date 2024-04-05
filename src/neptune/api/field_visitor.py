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
__all__ = ("FieldToValueVisitor",)

from datetime import datetime
from typing import (
    Any,
    Optional,
    Set,
)

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
    NotebookRefField,
    ObjectStateField,
    StringField,
    StringSeriesField,
    StringSetField,
)
from neptune.exceptions import MetadataInconsistency


class FieldToValueVisitor(FieldVisitor[Any]):

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
        raise MetadataInconsistency("Cannot get value for file attribute. Use download() instead.")

    def visit_file_set(self, field: FileSetField) -> None:
        raise MetadataInconsistency("Cannot get value for file set attribute. Use download() instead.")

    def visit_float_series(self, field: FloatSeriesField) -> Optional[float]:
        return field.last

    def visit_string_series(self, field: StringSeriesField) -> Optional[str]:
        return field.last

    def visit_image_series(self, field: ImageSeriesField) -> None:
        raise MetadataInconsistency("Cannot get value for image series.")

    def visit_string_set(self, field: StringSetField) -> Set[str]:
        return field.values

    def visit_git_ref(self, field: GitRefField) -> Optional[str]:
        return field.commit.commit_id if field.commit is not None else None

    def visit_object_state(self, field: ObjectStateField) -> str:
        return field.value

    def visit_notebook_ref(self, field: NotebookRefField) -> Optional[str]:
        return field.notebook_name

    def visit_artifact(self, field: ArtifactField) -> str:
        return field.hash
