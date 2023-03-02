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
from __future__ import annotations

__all__ = ["ArtifactFileType", "ArtifactMetadataSerializer", "ArtifactFileData", "ArtifactDriversMap", "ArtifactDriver"]

import enum
import pathlib
from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import (
    Dict,
    List,
    Optional,
    Type,
)

from neptune.exceptions import (
    NeptuneUnhandledArtifactSchemeException,
    NeptuneUnhandledArtifactTypeException,
)


class ArtifactFileType(enum.Enum):
    S3 = "S3"
    LOCAL = "Local"


class ArtifactMetadataSerializer:
    @staticmethod
    def serialize(metadata: Dict[str, str]) -> List[Dict[str, str]]:
        return [{"key": k, "value": v} for k, v in sorted(metadata.items())]

    @staticmethod
    def deserialize(metadata: List[Dict[str, str]]) -> Dict[str, str]:
        return {f'{key_value.get("key")}': f'{key_value.get("value")}' for key_value in metadata}


@dataclass
class ArtifactFileData:
    file_path: str
    file_hash: str
    type: str
    metadata: Dict[str, str]
    size: Optional[int] = None

    @classmethod
    def from_dto(cls, artifact_file_dto) -> "ArtifactFileData":
        return cls(
            file_path=artifact_file_dto.filePath,
            file_hash=artifact_file_dto.fileHash,
            type=artifact_file_dto.type,
            size=artifact_file_dto.size,
            metadata=ArtifactMetadataSerializer.deserialize(
                [{"key": str(m.key), "value": str(m.value)} for m in artifact_file_dto.metadata]
            ),
        )

    def to_dto(self) -> Dict[str, str | Optional[int] | List[Dict[str, str]]]:
        return {
            "filePath": self.file_path,
            "fileHash": self.file_hash,
            "type": self.type,
            "size": self.size,
            "metadata": ArtifactMetadataSerializer.serialize(self.metadata),
        }


class ArtifactDriversMap:
    _implementations: List[Type["ArtifactDriver"]] = []

    @classmethod
    def match_path(cls, path: str) -> Type["ArtifactDriver"]:
        for artifact_driver in cls._implementations:
            if artifact_driver.matches(path):
                return artifact_driver

        raise NeptuneUnhandledArtifactSchemeException(path)

    @classmethod
    def match_type(cls, type_str: str) -> Type["ArtifactDriver"]:
        for artifact_driver in cls._implementations:
            if artifact_driver.get_type() == type_str:
                return artifact_driver

        raise NeptuneUnhandledArtifactTypeException(type_str)


class ArtifactDriver(ABC):
    def __init_subclass__(cls) -> None:
        ArtifactDriversMap._implementations.append(cls)

    @staticmethod
    @abstractmethod
    def get_type() -> str:
        ...

    @classmethod
    @abstractmethod
    def matches(cls, path: str) -> bool:
        ...

    @classmethod
    @abstractmethod
    def get_tracked_files(cls, path: str, destination: Optional[str] = None) -> List[ArtifactFileData]:
        ...

    @classmethod
    @abstractmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData) -> None:
        ...
