#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["MetadataFile"]

import json
import os
from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

from neptune.core.components.abstract import WithResources

METADATA_FILENAME: str = "metadata.json"


def read_or_default(metadata_path: Path) -> Dict[str, Any]:
    if metadata_path.exists():
        with open(metadata_path, "r") as handler:
            data: Dict[str, Any] = json.load(handler)
            return data

    return dict()


class MetadataFile(WithResources):
    def __init__(self, data_path: Path, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._metadata_path: Path = (data_path / METADATA_FILENAME).resolve(strict=False)
        self._data: Dict[str, Any] = read_or_default(self._metadata_path)

        if metadata:
            for key, value in metadata.items():
                self.__setitem__(key, value)
            self.flush()

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def flush(self) -> None:
        with open(self._metadata_path, "w") as handler:
            json.dump(self._data, handler, indent=2)

    def clean(self) -> None:
        try:
            os.remove(self._metadata_path)
        except OSError:
            pass
