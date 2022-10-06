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

ARTIFACT_ATTRIBUTE_SPACE = "artifacts/"

LOG_ATTRIBUTE_SPACE = "logs/"

MONITORING_ATTRIBUTE_SPACE = "monitoring/"
MONITORING_STDERR_ATTRIBUTE_PATH = f"{MONITORING_ATTRIBUTE_SPACE}stderr"
MONITORING_STDOUT_ATTRIBUTE_PATH = f"{MONITORING_ATTRIBUTE_SPACE}stdout"
MONITORING_TRACEBACK_ATTRIBUTE_PATH = f"{MONITORING_ATTRIBUTE_SPACE}traceback"

PARAMETERS_ATTRIBUTE_SPACE = "parameters/"

PROPERTIES_ATTRIBUTE_SPACE = "properties/"

SOURCE_CODE_ATTRIBUTE_SPACE = "source_code/"
SOURCE_CODE_ENTRYPOINT_ATTRIBUTE_PATH = f"{SOURCE_CODE_ATTRIBUTE_SPACE}entrypoint"
SOURCE_CODE_FILES_ATTRIBUTE_PATH = f"{SOURCE_CODE_ATTRIBUTE_SPACE}files"

SYSTEM_ATTRIBUTE_SPACE = "sys/"
SYSTEM_DESCRIPTION_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}description"
SYSTEM_HOSTNAME_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}hostname"
SYSTEM_NAME_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}name"
SYSTEM_STATE_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}state"
SYSTEM_TAGS_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}tags"
SYSTEM_FAILED_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}failed"
SYSTEM_STAGE_ATTRIBUTE_PATH = f"{SYSTEM_ATTRIBUTE_SPACE}stage"

SIGNAL_TYPE_STOP = "neptune/stop"
SIGNAL_TYPE_ABORT = "neptune/abort"
