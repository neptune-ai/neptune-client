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
import typing
import pathlib
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError

from neptune.new.internal.artifacts.types import ArtifactDriver, ArtifactFileData, ArtifactFileType
from neptune.new.exceptions import (
    NeptuneRemoteStorageAccessException,
    NeptuneRemoteStorageCredentialsException
)


class S3ArtifactDriver(ArtifactDriver):
    @staticmethod
    def get_type() -> str:
        return "S3"

    @classmethod
    def matches(cls, path: str) -> bool:
        return urlparse(path).scheme == 's3'

    @classmethod
    def get_tracked_files(cls, path: str, name: str = None) -> typing.List[ArtifactFileData]:
        url = urlparse(path)
        bucket_name, prefix = url.netloc, url.path

        # pylint: disable=no-member
        remote_storage = boto3.resource('s3').Bucket(bucket_name)

        stored_files: typing.List[ArtifactFileData] = list()

        try:
            for remote_object in remote_storage.objects.filter(Prefix=prefix):
                remote_key = pathlib.Path(name or '') / pathlib.Path(remote_object.key[len(prefix):])

                stored_files.append(
                    ArtifactFileData(
                        file_path=str(remote_key),
                        file_hash=remote_object.e_tag.strip('"'),
                        type=ArtifactFileType.S3.value,
                        metadata={
                            "location": f's3://{bucket_name}{remote_object.key}',
                            "last_modified": remote_object.last_modified,
                            "file_size": remote_object.size
                        }
                    )
                )
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (remote_storage.meta.client.exceptions.NoSuchBucket,
                remote_storage.meta.client.exceptions.NoSuchBucket):
            raise NeptuneRemoteStorageAccessException(location=path)

        return stored_files

    @classmethod
    def download_file(cls, destination: pathlib.Path, file_definition: ArtifactFileData):
        location = file_definition.metadata.get('location')
        url = urlparse(location)
        bucket_name, path = url.netloc, url.path

        remote_storage = boto3.resource('s3')

        try:
            # pylint: disable=no-member
            bucket = remote_storage.Bucket(bucket_name)
            bucket.download_file(path, str(destination))
        except NoCredentialsError:
            raise NeptuneRemoteStorageCredentialsException()
        except (remote_storage.meta.client.exceptions.NoSuchBucket,
                remote_storage.meta.client.exceptions.NoSuchBucket):
            raise NeptuneRemoteStorageAccessException(location=location)
