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


class NeptuneException(Exception):
    pass


class MetadataInconsistency(NeptuneException):
    pass


class InternalClientError(NeptuneException):
    def __init__(self, msg: str):
        super().__init__("Internal client error: {}. Please contact Neptune support.".format(msg))


class ExperimentUUIDNotFound(NeptuneException):
    def __init__(self, exp_uuid: uuid.UUID):
        super().__init__("Experiment with UUID {} not found. Could be deleted.".format(exp_uuid))
