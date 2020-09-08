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

from typing import TypeVar, Iterable, TYPE_CHECKING

from neptune.internal.utils.images import ImageAcceptedTypes, get_image_content
from neptune.types.series.series import Series

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar('Ret')


class ImageSeries(Series):

    def __init__(self, values: Iterable[ImageAcceptedTypes]):
        self.values = [get_image_content(value) for value in values]

    def accept(self, visitor: 'ValueVisitor[Ret]') -> Ret:
        return visitor.visit_image_series(self)

    def __str__(self):
        return "ImageSeries({})".format(str(self.values))
