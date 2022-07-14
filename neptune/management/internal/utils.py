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
import random
import re
import string
from typing import Optional

from neptune.management.exceptions import (
    ConflictingWorkspaceName,
    InvalidProjectName,
    MissingWorkspaceName,
)
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN


def extract_project_and_workspace(name: str, workspace: Optional[str] = None):
    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, name)

    if not project_spec:
        raise InvalidProjectName(name=name)

    extracted_workspace, extracted_project_name = (
        project_spec["workspace"],
        project_spec["project"],
    )

    if not workspace and not extracted_workspace:
        raise MissingWorkspaceName(name=name)

    if workspace and extracted_workspace and workspace != extracted_workspace:
        raise ConflictingWorkspaceName(name=name, workspace=workspace)

    final_workspace_name = extracted_workspace or workspace

    return final_workspace_name, extracted_project_name


def normalize_project_name(name: str, workspace: Optional[str] = None):
    extracted_workspace_name, extracted_project_name = extract_project_and_workspace(
        name=name, workspace=workspace
    )

    return f"{extracted_workspace_name}/{extracted_project_name}"


class ProjectKeyGenerator:
    _PROJECT_KEY_MIN_LENGTH_FOR_DEFAULT_GENERATOR = 3  # project_name is at least 3 characters long
    _PROJECT_KEY_MAX_LENGTH_FOR_DEFAULT_GENERATOR = 5
    _PROJECT_KEY_MIN_NUMBER_ITERATOR = 2
    _PROJECT_KEY_MAX_NUMBER_ITERATOR = 3
    _PROJECT_KEY_RANDOM_SUFFIX_SIZE = 3
    _PROJECT_KEY_RANDOM_SUFFIX_ALPHABET = list(string.ascii_uppercase)

    def __init__(self, project_name: str, existing_project_keys: set[str]):
        self.project_name = project_name
        self.existing_project_keys = existing_project_keys
        self.max_substring_length = min(
            len(project_name), self._PROJECT_KEY_MAX_LENGTH_FOR_DEFAULT_GENERATOR
        )

    def get_default_project_key(self) -> str:
        unique_key = self._find_unique_project_key("")
        if unique_key is not None:
            return unique_key

        #  if above code do not find a proper id then add incrementation
        unique_key_with_incrementation = self._find_unique_project_key_with_incrementation()
        if unique_key_with_incrementation is not None:
            return unique_key_with_incrementation

        #  if above code do not find a proper id then add random string as suffix
        while 1:
            project_name_substring = (
                self.project_name[: self.max_substring_length] + self._generate_random_suffix()
            )
            if project_name_substring not in self.existing_project_keys:
                return project_name_substring

    def _generate_random_suffix(self):
        alphabet_copy = self._PROJECT_KEY_RANDOM_SUFFIX_ALPHABET.copy()
        random.shuffle(alphabet_copy)
        return "".join(alphabet_copy[: self._PROJECT_KEY_RANDOM_SUFFIX_SIZE])

    def _find_unique_project_key_with_incrementation(self) -> Optional[str]:
        for name_incrementation in range(
            self._PROJECT_KEY_MIN_NUMBER_ITERATOR, self._PROJECT_KEY_MAX_NUMBER_ITERATOR + 1
        ):
            unique_key = self._find_unique_project_key(str(name_incrementation))
            if unique_key is not None:
                return unique_key
        return None

    def _find_unique_project_key(self, additional_suffix: str = "") -> Optional[str]:
        for sub_string_length in range(
            self._PROJECT_KEY_MIN_LENGTH_FOR_DEFAULT_GENERATOR, self.max_substring_length + 1
        ):
            project_name_substring = self.project_name[:sub_string_length] + additional_suffix
            if project_name_substring not in self.existing_project_keys:
                return project_name_substring
        return None
