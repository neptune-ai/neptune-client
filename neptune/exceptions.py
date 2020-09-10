#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import uuid
from typing import Union, Optional

from packaging.version import Version

from neptune import envs
from neptune.internal.utils import replace_patch_version


class NeptuneException(Exception):
    pass


class NeptuneApiException(NeptuneException):
    pass


class MetadataInconsistency(NeptuneException):
    pass


class MalformedOperation(NeptuneException):
    pass


class FileNotFound(NeptuneException):
    def __init__(self, file: str):
        super().__init__("File not found: {}".format(file))


class InternalClientError(NeptuneException):
    def __init__(self, msg: str):
        super().__init__("Internal client error: {}. Please contact Neptune support.".format(msg))


class BadUsage(NeptuneException):
    def __init__(self, msg: str):
        super().__init__("Bad usage: {}".format(msg))


class ProjectNotFound(NeptuneException):
    def __init__(self, project_id):
        super().__init__("Project {} not found.".format(project_id))


class ExperimentUUIDNotFound(NeptuneException):
    def __init__(self, exp_uuid: uuid.UUID):
        super().__init__("Experiment with UUID {} not found. Could be deleted.".format(exp_uuid))


class MissingProject(NeptuneException):
    def __init__(self):
        super().__init__('Missing project identifier. Use "{}" environment variable or pass it as an argument'.format(
            envs.PROJECT_ENV_NAME))


class MissingApiToken(NeptuneException):
    def __init__(self):
        super().__init__(
            'Missing API token. Use "{}" environment variable or pass it as an argument to neptune.init. '
            'Open this link to get your API token https://ui.neptune.ai/get_my_api_token'.format(
                envs.API_TOKEN_ENV_NAME))


class InvalidApiKey(NeptuneException):
    def __init__(self):
        super().__init__('The provided API key is invalid.')


class UnsupportedClientVersion(NeptuneException):
    def __init__(
            self,
            version: Union[Version, str],
            min_version: Optional[Union[Version, str]] = None,
            max_version: Optional[Union[Version, str]] = None):
        super().__init__(
            "This neptune-client version ({}) is not supported. Please install neptune-client{}".format(
                str(version),
                "==" + replace_patch_version(str(max_version)) if max_version else ">=" + str(min_version)
            ))


class CannotResolveHostname(NeptuneException):
    def __init__(self, host):
        super().__init__("Cannot resolve hostname {}.".format(host))


class SSLError(NeptuneException):
    def __init__(self):
        super().__init__(
            'SSL certificate validation failed. Set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE '
            'environment variable to accept self-signed certificates.')


class ConnectionLost(NeptuneException):
    def __init__(self):
        super().__init__('Connection to Neptune server was lost.')


class InternalServerError(NeptuneApiException):
    def __init__(self):
        super().__init__('Internal server error. Please contact Neptune support.')


class Unauthorized(NeptuneApiException):
    def __init__(self):
        super().__init__('Unauthorized. Verify your API token is invalid.')


class Forbidden(NeptuneApiException):
    def __init__(self):
        super().__init__('You have no permissions to access this resource.')
