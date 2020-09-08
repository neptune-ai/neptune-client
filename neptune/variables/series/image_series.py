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

import time
from typing import Optional

from neptune.internal.utils import verify_type
from neptune.internal.utils.images import get_image_content

from neptune.types.series.image_series import ImageSeries as ImageSeriesVal, ImageAcceptedTypes

from neptune.internal.operation import LogImages, ClearImageLog
from neptune.variables.series.series import Series


class ImageSeries(Series):

    def assign(self, value: ImageSeriesVal, wait: bool = False):
        verify_type("value", value, ImageSeriesVal)

        with self._experiment.lock():
            clear_op = ClearImageLog(self._experiment_uuid, self._path)
            if not value.values:
                self._enqueue_operation(clear_op, wait=wait)
            else:
                self._enqueue_operation(clear_op, wait=False)
                ts = time.time()
                values = [LogImages.ValueType(val, step=None, ts=ts) for val in value.values]
                self._enqueue_operation(LogImages(self._experiment_uuid, self._path, values), wait=wait)

    def log(self,
            value: ImageAcceptedTypes,
            step: Optional[float] = None,
            timestamp: Optional[float] = None,
            wait: bool = False):
        verify_type("step", step, (float, int, type(None)))
        verify_type("timestamp", timestamp, (float, int, type(None)))

        if not timestamp:
            timestamp = time.time()

        content = get_image_content(value)

        with self._experiment.lock():
            self._enqueue_operation(
                LogImages(self._experiment_uuid, self._path, [LogImages.ValueType(content, step, timestamp)]),
                wait)

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(ClearImageLog(self._experiment_uuid, self._path), wait)
