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
from types import TracebackType
from typing import (
    Any,
    Dict,
    Optional,
    Type,
)


class MetadataFile:
    METADATA_FILE: str = "metadata.json"

    def __init__(self, data_path: Path, metadata: Optional[Dict[str, Any]] = None):
        # initialize directory
        os.makedirs(data_path, exist_ok=True)

        self._metadata_path: Path = (data_path / MetadataFile.METADATA_FILE).resolve(strict=False)
        self._data: Dict[str, Any] = self._read_or_default()

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

    def _read_or_default(self) -> Dict[str, Any]:
        if self._metadata_path.exists():
            with open(self._metadata_path, "r") as handler:
                data: Dict[str, Any] = json.load(handler)
                return data

        return dict()

    def close(self) -> None:
        self.flush()

    def cleanup(self) -> None:
        try:
            os.remove(self._metadata_path)
        except OSError:
            pass

    def __enter__(self) -> "MetadataFile":
        return self

    def __exit__(
        self,
        exc_type: Type[Optional[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()
