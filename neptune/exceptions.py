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
from neptune import envs


class NeptuneException(Exception):
    pass


class Uninitialized(NeptuneException):
    def __init__(self):
        super(Uninitialized, self).__init__(
            "You must initialize neptune-client first. "
            "For more information, please visit: https://github.com/neptune-ml/neptune-client#initialize-neptune")


class FileNotFound(NeptuneException):
    def __init__(self, path):
        super(FileNotFound, self).__init__("File {} doesn't exist.".format(path))


class NotAFile(NeptuneException):
    def __init__(self, path):
        super(NotAFile, self).__init__("Path {} is not a file.".format(path))


class InvalidNotebookPath(NeptuneException):
    def __init__(self, path):
        super(InvalidNotebookPath, self).__init__(
            "File {} is not a valid notebook. Should end with .ipynb.".format(path))


class InvalidChannelX(NeptuneException):
    def __init__(self, x):
        super(InvalidChannelX, self).__init__(
            "Invalid channel X-coordinate: '{}'. The sequence of X-coordinates must be strictly increasing.".format(x))


class NoChannelValue(NeptuneException):
    def __init__(self):
        super(NoChannelValue, self).__init__('No channel value provided.')


class LibraryNotInstalled(NeptuneException):
    def __init__(self, library):
        super(LibraryNotInstalled, self).__init__("Library {} is not installed".format(library))


class InvalidChannelValue(NeptuneException):
    def __init__(self, expected_type, actual_type):
        super(InvalidChannelValue, self).__init__(
            'Invalid channel value type. Expected: {expected}, actual: {actual}.'.format(
                expected=expected_type, actual=actual_type))


class NoExperimentContext(NeptuneException):
    def __init__(self):
        super(NoExperimentContext, self).__init__('Unable to find current active experiment')


class MissingApiToken(NeptuneException):
    def __init__(self):
        super(MissingApiToken, self).__init__('Missing API token. Use "{}" environment '
                                              'variable or pass it as an argument'
                                              .format(envs.API_TOKEN_ENV_NAME))


class MissingProjectQualifiedName(NeptuneException):
    def __init__(self):
        super(MissingProjectQualifiedName, self).__init__('Missing project qualified name. Use "{}" environment '
                                                          'variable or pass it as an argument'
                                                          .format(envs.PROJECT_ENV_NAME))


class IncorrectProjectQualifiedName(NeptuneException):
    def __init__(self, project_qualified_name):
        super(IncorrectProjectQualifiedName, self).__init__('Incorrect project qualified name "{}". '
                                                            'Should be in format "namespace/project_name".'
                                                            .format(project_qualified_name))
