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
import os.path
import sys
from typing import Tuple, List, Optional

from neptune.internal.storage.storage_utils import normalize_file_name
from neptune.utils import glob, is_ipython


def get_source_code_to_upload(upload_source_files: Optional[List[str]]) -> Tuple[str, List[Tuple[str, str]]]:
    source_target_pairs = []
    if is_ipython():
        main_file = None
        entrypoint = None
    else:
        main_file = sys.argv[0]
        entrypoint = main_file or None
    if upload_source_files is None:
        if main_file is not None and os.path.isfile(main_file):
            entrypoint = normalize_file_name(os.path.basename(main_file))
            source_target_pairs = [
                (os.path.abspath(main_file), normalize_file_name(os.path.basename(main_file)))
            ]
    else:
        expanded_source_files = set()
        for filepath in upload_source_files:
            expanded_source_files |= set(glob(filepath))
        if sys.version_info.major < 3 or (sys.version_info.major == 3 and sys.version_info.minor < 5):
            for filepath in expanded_source_files:
                if filepath.startswith('..'):
                    raise ValueError('You need to have Python 3.5 or later to use paths outside current directory.')
                source_target_pairs.append((os.path.abspath(filepath), normalize_file_name(filepath)))
        else:
            absolute_paths = []
            for filepath in expanded_source_files:
                absolute_paths.append(os.path.abspath(filepath))
            try:
                common_source_root = os.path.commonpath(absolute_paths)
            except ValueError:
                for absolute_path in absolute_paths:
                    source_target_pairs.append((absolute_path, normalize_file_name(absolute_path)))
            else:
                if os.path.isfile(common_source_root):
                    common_source_root = os.path.dirname(common_source_root)
                if common_source_root.startswith(os.getcwd() + os.sep):
                    common_source_root = os.getcwd()
                for absolute_path in absolute_paths:
                    source_target_pairs.append((absolute_path, normalize_file_name(
                        os.path.relpath(absolute_path, common_source_root))))
    return entrypoint, source_target_pairs
