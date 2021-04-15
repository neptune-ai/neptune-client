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
import imghdr
import os
import pathlib

from typing import Optional, Iterable

from neptune.new.internal.utils import base64_encode

from neptune.new.exceptions import FileNotFound, OperationNotSupported

from neptune.new.types import File
from neptune.new.types.series.file_series import FileSeries as FileSeriesVal
from neptune.new.internal.operation import ImageValue, LogImages, ClearImageLog, Operation
from neptune.new.attributes.series.series import Series

Val = FileSeriesVal
Data = File


class FileSeries(Series[Val, Data]):

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [
            LogImages.ValueType(
                ImageValue(data=self._get_base64_image_content(val), name=value.name, description=value.description),
                step=step,
                ts=timestamp)
            for val in value.values
        ]
        return LogImages(self._path, values)

    def _get_clear_operation(self) -> Operation:
        return ClearImageLog(self._path)

    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        return FileSeriesVal(values, **kwargs)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, FileSeriesVal)

    @staticmethod
    def _get_base64_image_content(file: File) -> str:
        if file.path is not None:
            if not os.path.exists(file.path):
                raise FileNotFound(file.path)
            with open(file.path, 'rb') as image_file:
                file = File.from_stream(image_file)

        ext = imghdr.what("", h=file.content)
        if not ext:
            raise OperationNotSupported("FileSeries supports only image files for now. "
                                        "Other file types will be implemented in future.")

        return base64_encode(file.content)

    def download(self, destination: Optional[str]):
        target_dir = self._get_destination(destination)
        item_count = self._backend.get_image_series_values(self._run_uuid, self._path, 0, 1).totalItemCount
        for i in range(0, item_count):
            self._backend.download_file_series_by_index(self._run_uuid, self._path, i, target_dir)

    def download_last(self, destination: Optional[str]):
        target_dir = self._get_destination(destination)
        item_count = self._backend.get_image_series_values(self._run_uuid, self._path, 0, 1).totalItemCount
        if item_count > 0:
            self._backend.download_file_series_by_index(self._run_uuid, self._path, item_count - 1, target_dir)
        else:
            raise ValueError("Unable to download last file - series is empty")

    def _get_destination(self, destination: Optional[str]):
        target_dir = destination
        if destination is None:
            target_dir = os.path.join('neptune', self._path[-1])
        pathlib.Path(os.path.abspath(target_dir)).mkdir(parents=True, exist_ok=True)
        return target_dir
