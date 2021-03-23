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
import logging
import os
import sys
from typing import Optional, List

from neptune.new import Run
from neptune.new.attributes import constants as attr_consts
from neptune.new.internal.utils import get_absolute_paths, get_common_root
from neptune.internal.storage.storage_utils import normalize_file_name
from neptune.utils import is_ipython

_logger = logging.getLogger(__name__)


def upload_source_code(source_files: Optional[List[str]], run: Run) -> None:
    if not is_ipython() and os.path.isfile(sys.argv[0]):
        if source_files is None:
            entrypoint = os.path.basename(sys.argv[0])
            source_files = sys.argv[0]
        elif not source_files:
            entrypoint = os.path.basename(sys.argv[0])
        else:
            common_root = get_common_root(get_absolute_paths(source_files))
            if common_root is not None:
                entrypoint = normalize_file_name(os.path.relpath(os.path.abspath(sys.argv[0]), common_root))
            else:
                entrypoint = normalize_file_name(os.path.abspath(sys.argv[0]))
        run[attr_consts.SOURCE_CODE_ENTRYPOINT_ATTRIBUTE_PATH] = entrypoint

    if source_files is not None:
        run[attr_consts.SOURCE_CODE_FILES_ATTRIBUTE_PATH].upload_files(source_files)
