#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
from neptune.exceptions import NeptuneException


class NeptuneApiException(NeptuneException):
    pass


class SSLError(NeptuneException):
    def __init__(self):
        super(SSLError, self).__init__('SSL certificate validation failed. Set NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE '
                                       'environment variable to accept self-signed certificates.')


class ConnectionLost(NeptuneApiException):
    def __init__(self):
        super(ConnectionLost, self).__init__('Connection lost. Please try again.')


class ServerError(NeptuneApiException):
    def __init__(self):
        super(ServerError, self).__init__('Server error. Please try again later.')


class Unauthorized(NeptuneApiException):
    def __init__(self):
        super(Unauthorized, self).__init__('Your API token is invalid.')


class Forbidden(NeptuneApiException):
    def __init__(self):
        super(Forbidden, self).__init__('You have no permissions to access this resource.')


class InvalidApiKey(NeptuneApiException):
    def __init__(self):
        super(InvalidApiKey, self).__init__('The provided API key is invalid.')


class NamespaceNotFound(NeptuneApiException):
    def __init__(self, namespace_name):
        super(NamespaceNotFound, self).__init__("Namespace '{}' not found.".format(namespace_name))


class ProjectNotFound(NeptuneApiException):
    def __init__(self, project_identifier):
        super(ProjectNotFound, self).__init__("Project '{}' not found.".format(project_identifier))


class ExperimentNotFound(NeptuneApiException):
    def __init__(self, experiment_short_id, project_qualified_name):
        super(ExperimentNotFound, self).__init__("Experiment '{exp}' not found in '{project}'.".format(
            exp=experiment_short_id, project=project_qualified_name))


class ExperimentAlreadyFinished(NeptuneApiException):
    def __init__(self, experiment_short_id):
        super(ExperimentAlreadyFinished, self).__init__(
            "Experiment '{}' is already finished.".format(experiment_short_id))


class ExperimentLimitReached(NeptuneApiException):
    def __init__(self):
        super(ExperimentLimitReached, self).__init__('Experiment limit reached.')


class StorageLimitReached(NeptuneApiException):
    def __init__(self):
        super(StorageLimitReached, self).__init__('Storage limit reached.')


class ExperimentValidationError(NeptuneApiException):
    pass


class ChannelAlreadyExists(NeptuneApiException):
    def __init__(self, experiment_short_id, channel_name):
        super(ChannelAlreadyExists, self).__init__(
            "Channel with name '{}' already exists in experiment '{}'.".format(channel_name, experiment_short_id))


class ChannelsValuesSendBatchError(NeptuneApiException):
    def __init__(self, experiment_short_id, batch_errors):
        super(ChannelsValuesSendBatchError, self) \
            .__init__("Received batch errors sending channels' values to experiment {}. "
                      "Skipping {} values.".format(experiment_short_id, len(batch_errors)))
