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
__all__ = ["FileHasher"]

import datetime
import hashlib
import typing
from pathlib import Path

from neptune.new.internal.artifacts.local_file_hash_storage import LocalFileHashStorage
from neptune.new.internal.artifacts.types import (
    ArtifactFileData,
    ArtifactMetadataSerializer,
)
from neptune.new.internal.artifacts.utils import sha1


class FileHasher:
    ENCODING = "UTF-8"
    HASH_ELEMENT_DIVISOR = b"#"
    META_ELEMENT_DIVISOR = b"|"
    SERVER_INT_BYTES = 4
    SERVER_LONG_BYTES = 8
    SERVER_BYTE_ORDER = "big"
    HASH_LENGTH = 64  # sha-256

    @classmethod
    def get_local_file_hash(cls, file_path: typing.Union[str, Path]) -> str:
        local_storage = LocalFileHashStorage()

        absolute = Path(file_path).resolve()
        modification_date = datetime.datetime.fromtimestamp(absolute.stat().st_mtime).strftime("%Y%m%d_%H%M%S%f")

        stored_file_hash = local_storage.fetch_one(absolute)

        if stored_file_hash:
            if stored_file_hash.modification_date >= modification_date:
                return stored_file_hash.file_hash
            else:
                computed_hash = sha1(absolute)
                local_storage.update(absolute, computed_hash, modification_date)

                return computed_hash
        else:
            computed_hash = sha1(absolute)
            local_storage.insert(absolute, computed_hash, modification_date)

            return computed_hash

    @classmethod
    def _number_to_bytes(cls, int_value: int, bytes_cnt):
        return int_value.to_bytes(bytes_cnt, cls.SERVER_BYTE_ORDER)

    @classmethod
    def get_artifact_hash(cls, artifact_files: typing.Iterable[ArtifactFileData]) -> str:
        artifact_hash = hashlib.sha256()

        for artifact_file in sorted(artifact_files, key=lambda file: file.file_path):
            artifact_hash.update(cls.HASH_ELEMENT_DIVISOR)
            artifact_hash.update(cls._number_to_bytes(len(artifact_file.file_path), cls.SERVER_INT_BYTES))
            artifact_hash.update(artifact_file.file_path.encode(cls.ENCODING))
            artifact_hash.update(cls.HASH_ELEMENT_DIVISOR)
            artifact_hash.update(artifact_file.file_hash.encode(cls.ENCODING))
            artifact_hash.update(cls.HASH_ELEMENT_DIVISOR)
            if artifact_file.size is not None:
                artifact_hash.update(cls._number_to_bytes(artifact_file.size, cls.SERVER_LONG_BYTES))
            artifact_hash.update(cls.HASH_ELEMENT_DIVISOR)
            artifact_hash.update(cls._number_to_bytes(len(artifact_file.type), cls.SERVER_INT_BYTES))
            artifact_hash.update(artifact_file.type.encode(cls.ENCODING))
            artifact_hash.update(cls.HASH_ELEMENT_DIVISOR)
            for metadata_key_value in ArtifactMetadataSerializer.serialize(artifact_file.metadata):
                metadata_name, metadata_value = metadata_key_value.get("key"), metadata_key_value.get("value")
                artifact_hash.update(cls.META_ELEMENT_DIVISOR)
                artifact_hash.update(cls._number_to_bytes(len(metadata_name), cls.SERVER_INT_BYTES))
                artifact_hash.update(metadata_name.encode(cls.ENCODING))
                artifact_hash.update(cls.META_ELEMENT_DIVISOR)
                artifact_hash.update(cls._number_to_bytes(len(metadata_value), cls.SERVER_INT_BYTES))
                artifact_hash.update(metadata_value.encode(cls.ENCODING))

        return str(artifact_hash.hexdigest())
