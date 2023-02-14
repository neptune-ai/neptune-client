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
# flake8: noqa
from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.utils import (
    IS_MACOS,
    IS_WINDOWS,
    NoopObject,
    align_channels_on_x,
    as_list,
    assure_directory_exists,
    assure_project_qualified_name,
    discover_git_repo_location,
    file_contains,
    get_channel_name_stems,
    get_git_info,
    glob,
    in_docker,
    is_float,
    is_ipython,
    is_nan_or_inf,
    is_notebook,
    map_keys,
    map_values,
    merge_dataframes,
    update_session_proxies,
    validate_notebook_path,
)
