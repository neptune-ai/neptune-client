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

# backwards compatibility
# pylint: disable=unused-import

from neptune.new.attributes import File
from neptune.new.attributes.atoms.artifact import Artifact
from neptune.new.attributes.file_set import FileSet
from neptune.new.attributes.series import FileSeries
from neptune.new.attributes.series.float_series import FloatSeries
from neptune.new.attributes.series.string_series import StringSeries
from neptune.new.attributes.sets.string_set import StringSet
from neptune.new.exceptions import NeptuneException
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.utils import (
    verify_type,
    is_collection,
    verify_collection_type,
    is_float,
    is_string,
    is_float_like,
    is_string_like,
)
from neptune.new.internal.utils.paths import join_paths, parse_path
from neptune.new.types.atoms.file import File as FileVal
from neptune.new.types.value_copy import ValueCopy

from neptune.new.attributes_containers import Handler
