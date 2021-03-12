#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

from typing import TypeVar, Iterable, TYPE_CHECKING, List

from neptune.new.types import File

from neptune.new.internal.utils import verify_collection_type

from neptune.new.types.series.series import Series

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar('Ret')


class FileSeries(Series):

    def __init__(self, values: Iterable[File]):
        verify_collection_type("values", values, File)
        self._values = list(values)

    def accept(self, visitor: 'ValueVisitor[Ret]') -> Ret:
        return visitor.visit_image_series(self)

    @property
    def values(self) -> List[File]:
        return self._values

    def __str__(self):
        return "FileSeries({})".format(str(self.values))
