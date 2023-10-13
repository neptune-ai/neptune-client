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
__all__ = ["MetadataFile"]

import json
import os
from pathlib import Path
from typing import (
    IO,
    Any,
    Dict,
)


class MetadataFile:
    METADATA_FILE: str = "metadata.json"

    def __init__(self, data_path: Path):
        self._metadata_path: Path = (data_path / MetadataFile.METADATA_FILE).resolve()

        # initialize directory
        os.makedirs(data_path, exist_ok=True)

        self._file_handler: IO = open(self._metadata_path, "w")
        self._data: Dict[str, Any] = dict()

    @property
    def metadata_path(self) -> Path:
        return self._metadata_path

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def flush(self) -> None:
        json.dump(self._data, self._file_handler, indent=2)

    def close(self) -> None:
        self.flush()
        self._file_handler.close()

    def cleanup(self) -> None:
        try:
            os.remove(self._metadata_path)
        except OSError:
            pass
