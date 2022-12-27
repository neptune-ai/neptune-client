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
__all__ = ["get_boto_s3_client"]

import os

import boto3

from neptune.new.envs import S3_ENDPOINT_URL


def get_boto_s3_client():
    """
    User might want to use other than `AWS` `S3` providers, so we should be able to override `endpoint_url`.
    Unfortunately `boto3` doesn't support this parameter in configuration, so we'll have to create our env variable.
    boto3 supported config envs:
     * https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables
    boto3 `endpoint_url` support PR:
     * https://github.com/boto/boto3/pull/2746
    """
    endpoint_url = os.getenv(S3_ENDPOINT_URL)
    return boto3.resource(
        service_name="s3",
        endpoint_url=endpoint_url,
    )
