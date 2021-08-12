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
import pathlib
from datetime import datetime
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError

from neptune.new.internal.artifacts.types import ArtifactDriver, ArtifactFileData, ArtifactFileType
from neptune.new.exceptions import (
    NeptuneRemoteStorageAccessException,
    NeptuneRemoteStorageCredentialsException,
)


class S3ArtifactDriver(ArtifactDriver):
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def get_type() -> str:
        return "S3"

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme == 's3'

    @classmethod
    def _serialize_metadata(cls, metadata: typing.Dict[str, typing.Any]) -> typing.Dict[str, str]:
        return {
            "location": metadata['location'],
            "last_modified": metadata['last_modified'].strftime(cls.DATETIME_FORMAT),
            "file_size": str(metadata['file_size']),
        }

    @classmethod
    def _deserialize_metadata(cls, metadata: typing.Dict[str, str]) -> typing.Dict[str, typing.Any]:
        return {
            "location": metadata['location'],
            "last_modified": datetime.strptime(metadata['last_modified'], cls.DATETIME_FORMAT),
            "file_size": int(metadata['file_size']),
        }

    @classmethod
    def get_tracked_files(cls, path: str, name: str = None) -> typing.List[ArtifactFileData]:
        url = urlparse(path)
        bucket_name, prefix = url.netloc, url.path

        # pylint: disable=no-member
        remote_storage = boto3.resource('s3').Bucket(bucket_name)

        stored_files: typing.List[ArtifactFileData] = list()

        try:
            for remote_object in remote_storage.objects.filter(Prefix=prefix):
                # If prefix is path to file get only directories
                if prefix == remote_object.key:
                    prefix = str(pathlib.Path(prefix).parent)

                file_path = pathlib.Path(name or '') / pathlib.Path(remote_object.key[len(prefix):])
                remote_key = remote_object.key.lstrip('/')

                stored_files.append(
                    ArtifactFileData(
                        file_path=str(file_path).lstrip('/'),
                        file_hash=remote_object.e_tag.strip('"'),
                        type=ArtifactFileType.S3.value,
                        metadata=cls._serialize_metadata({
                            "location": f's3://{bucket_name}/{remote_key}',
                            "last_modified": remote_object.last_modified,
                            "file_size": remote_object.size,
                        })
                    )
                )
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (remote_storage.meta.client.exceptions.NoSuchBucket,
                remote_storage.meta.client.exceptions.NoSuchKey):
            raise NeptuneRemoteStorageAccessException(location=path)

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        location = file_definition.metadata.get('location')
        destination = destination / file_definition.file_path
        url = urlparse(location)
        bucket_name, path = url.netloc, url.path

        remote_storage = boto3.resource('s3')

        os.makedirs(str(destination.parent), exist_ok=True)

        try:
            # pylint: disable=no-member
            bucket = remote_storage.Bucket(bucket_name)
            bucket.download_file(path, str(destination))
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (remote_storage.meta.client.exceptions.NoSuchBucket,
                remote_storage.meta.client.exceptions.NoSuchKey):
            raise NeptuneRemoteStorageAccessException(location=location)
