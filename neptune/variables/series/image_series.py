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

from typing import Optional

from neptune.internal.utils import verify_type
from neptune.internal.utils.images import get_image_content

from neptune.types.series.image_series import ImageSeries as ImageSeriesVal, ImageAcceptedTypes

from neptune.internal.operation import LogImages, ClearImageLog, Operation
from neptune.variables.series.series import Series

Val = ImageSeriesVal
Data = ImageAcceptedTypes


class ImageSeries(Series[Val, Data]):

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [LogImages.ValueType(val, step=step, ts=timestamp) for val in value.values]
        return LogImages(self._path, values)

    def _get_log_operation_from_data(self, data: Data, step: Optional[float], timestamp: float) -> Operation:
        content = get_image_content(data)
        return LogImages(self._path, [LogImages.ValueType(content, step, timestamp)])

    def _get_clear_operation(self) -> Operation:
        return ClearImageLog(self._path)

    def _verify_value_type(self, value) -> None:
        verify_type("value", value, Val)
