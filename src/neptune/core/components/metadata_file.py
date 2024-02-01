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
__all__ = ["MetadataFile"]

import os
from json import (
    JSONDecodeError,
    dump,
    load,
)
from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

from neptune.core.components.abstract import Resource

METADATA_FILE: str = "metadata.json"


class MetadataFile(Resource):
    def __init__(self, data_path: Path, metadata: Optional[Dict[str, Any]] = None):
        self._data_path = data_path
        self._metadata_path: Path = (data_path / METADATA_FILE).resolve(strict=False)
        self._data: Dict[str, Any] = self._read_or_default()

        if metadata:
            for key, value in metadata.items():
                self.__setitem__(key, value)
            self.flush()

    @property
    def data_path(self) -> Path:
        return self._data_path

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def flush(self) -> None:
        with open(self._metadata_path, "w") as handler:
            dump(self._data, handler, indent=2)

    def _read_or_default(self) -> Dict[str, Any]:
        if self._metadata_path.exists():
            try:
                with open(self._metadata_path, "r") as handler:
                    data: Dict[str, Any] = load(handler)
                    return data
            except (OSError, JSONDecodeError):
                pass

        return dict()

    def cleanup(self) -> None:
        try:
            os.remove(self._metadata_path)
        except OSError:
            pass
