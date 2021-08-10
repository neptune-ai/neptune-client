#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import os
import typing
from pathlib import Path
from dataclasses import dataclass

from datalite import datalite


def persist(cls) -> typing.Type:
    def decorator(inner_cls, *args, **kwargs) -> typing.Type:
        path = Path.home() / ".neptune"
        os.makedirs(path, exist_ok=True)

        return datalite(db_path=str(path / "files.db"))(inner_cls, *args, **kwargs)
    return decorator(cls)


@persist
@dataclass
class LocalFileHash:
    file_path: str
    file_hash: str
    modification_date: str
