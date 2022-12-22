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
__all__ = ["S3ArtifactDriver"]

import pathlib
import typing
from datetime import datetime
from urllib.parse import urlparse

from botocore.exceptions import NoCredentialsError

from neptune.new.exceptions import (
    NeptuneRemoteStorageAccessException,
    NeptuneRemoteStorageCredentialsException,
    NeptuneUnsupportedArtifactFunctionalityException,
)
from neptune.new.internal.artifacts.types import (
    ArtifactDriver,
    ArtifactFileData,
    ArtifactFileType,
)
from neptune.new.internal.utils.s3 import get_boto_s3_client


class S3ArtifactDriver(ArtifactDriver):
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def get_type() -> str:
        return ArtifactFileType.S3.value

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme == "s3"

    @classmethod
    def _serialize_metadata(cls, metadata: typing.Dict[str, typing.Any]) -> typing.Dict[str, str]:
        return {
            "location": metadata["location"],
            "last_modified": metadata["last_modified"].strftime(cls.DATETIME_FORMAT),
        }

    @classmethod
    def _deserialize_metadata(cls, metadata: typing.Dict[str, str]) -> typing.Dict[str, typing.Any]:
        return {
            "location": metadata["location"],
            "last_modified": datetime.strptime(metadata["last_modified"], cls.DATETIME_FORMAT),
        }

    @classmethod
    def get_tracked_files(cls, path: str, destination: str = None) -> typing.List[ArtifactFileData]:
        url = urlparse(path)
        bucket_name, prefix = url.netloc, url.path.lstrip("/")

        if "*" in prefix:
            raise NeptuneUnsupportedArtifactFunctionalityException(
                f"Wildcard characters (*,?) in location URI ({path}) are not supported."
            )

        remote_storage = get_boto_s3_client().Bucket(bucket_name)

        stored_files: typing.List[ArtifactFileData] = list()

        try:
            for remote_object in remote_storage.objects.filter(Prefix=prefix):
                # If prefix is path to file get only directories
                if prefix == remote_object.key:
                    prefix = str(pathlib.PurePosixPath(prefix).parent)

                remote_key = remote_object.key
                destination = pathlib.PurePosixPath(destination or "")
                relative_file_path = remote_key[len(prefix.lstrip(".")) :].lstrip("/")

                file_path = destination / relative_file_path

                stored_files.append(
                    ArtifactFileData(
                        file_path=str(file_path).lstrip("/"),
                        file_hash=remote_object.e_tag.strip('"'),
                        type=ArtifactFileType.S3.value,
                        size=remote_object.size,
                        metadata=cls._serialize_metadata(
                            {
                                "location": f's3://{bucket_name}/{remote_key.lstrip("/")}',
                                "last_modified": remote_object.last_modified,
                            }
                        ),
                    )
                )
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (
            remote_storage.meta.client.exceptions.NoSuchBucket,
            remote_storage.meta.client.exceptions.NoSuchKey,
        ):
            raise NeptuneRemoteStorageAccessException(location=path)

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        location = file_definition.metadata.get("location")
        url = urlparse(location)
        bucket_name, path = url.netloc, url.path.lstrip("/")

        remote_storage = get_boto_s3_client()
        try:
            bucket = remote_storage.Bucket(bucket_name)
            bucket.download_file(path, str(destination))
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (
            remote_storage.meta.client.exceptions.NoSuchBucket,
            remote_storage.meta.client.exceptions.NoSuchKey,
        ):
            raise NeptuneRemoteStorageAccessException(location=location)
