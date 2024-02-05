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
__all__ = ["FileSeries"]

import io
import os
import pathlib
from typing import (
    Iterable,
    List,
    Optional,
)

from PIL import (
    Image,
    UnidentifiedImageError,
)

from neptune.attributes.series.series import Series
from neptune.exceptions import (
    FileNotFound,
    OperationNotSupported,
)
from neptune.internal.operation import (
    ClearImageLog,
    ImageValue,
    LogImages,
    Operation,
)
from neptune.internal.types.file_types import FileType
from neptune.internal.utils import base64_encode
from neptune.internal.utils.limits import image_size_exceeds_limit_for_logging
from neptune.types import File
from neptune.types.series.file_series import FileSeries as FileSeriesVal
from neptune.typing import ProgressBarType

Val = FileSeriesVal
Data = File
LogOperation = LogImages


class FileSeries(Series[Val, Data, LogOperation], max_batch_size=1, operation_cls=LogOperation):
    @classmethod
    def _map_series_val(cls, value: Val) -> List[ImageValue]:
        return [
            ImageValue(
                data=cls._get_base64_image_content(val),
                name=value.name,
                description=value.description,
            )
            for val in value.values
        ]

    def _get_clear_operation(self) -> Operation:
        return ClearImageLog(self._path)

    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        return FileSeriesVal(values, **kwargs)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, FileSeriesVal)

    @staticmethod
    def _get_base64_image_content(file: File) -> str:
        if file.file_type is FileType.LOCAL_FILE:
            if not os.path.exists(file.path):
                raise FileNotFound(file.path)
            with open(file.path, "rb") as image_file:
                file_content = File.from_stream(image_file).content
        else:
            file_content = file.content

        try:
            with Image.open(io.BytesIO(file_content)):
                ...
        except UnidentifiedImageError:
            raise OperationNotSupported(
                "FileSeries supports only image files for now. Other file types will be implemented in future."
            )

        if image_size_exceeds_limit_for_logging(len(file_content)):
            file_content = b""

        return base64_encode(file_content)

    def download(self, destination: Optional[str], progress_bar: Optional[ProgressBarType] = None):
        target_dir = self._get_destination(destination)
        item_count = self._backend.get_image_series_values(
            self._container_id, self._container_type, self._path, 0, 1
        ).totalItemCount
        for i in range(0, item_count):
            self._backend.download_file_series_by_index(
                self._container_id, self._container_type, self._path, i, target_dir, progress_bar
            )

    def download_last(self, destination: Optional[str]):
        target_dir = self._get_destination(destination)
        item_count = self._backend.get_image_series_values(
            self._container_id, self._container_type, self._path, 0, 1
        ).totalItemCount
        if item_count > 0:
            self._backend.download_file_series_by_index(
                self._container_id,
                self._container_type,
                self._path,
                item_count - 1,
                target_dir,
                progress_bar=None,
            )
        else:
            raise ValueError("Unable to download last file - series is empty")

    def _get_destination(self, destination: Optional[str]):
        target_dir = destination
        if destination is None:
            target_dir = os.path.join("neptune", self._path[-1])
        pathlib.Path(os.path.abspath(target_dir)).mkdir(parents=True, exist_ok=True)
        return target_dir
