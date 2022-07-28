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
from typing import Optional, Set

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
    """
    The first part algorithm checks whether project_name contains more than
    _PROJECT_KEY_MIN_LENGTH_FOR_DEFAULT_GENERATOR alphanumeric characters. If it is not true then project key is
    generated from project_name alphanumeric characters and random suffix of size _PROJECT_KEY_RANDOM_SUFFIX_SIZE.

    The second stage if unique key is not found is find the shortest unique non digit substring of projectName.
    Substring is always from index 0.

    The second stage if unique key is not found is digit addition at the end of the shortest unique non digit substring.
    Substring is always from index 0.

    The last stage if unique non digit key is not found, is addition of random suffix of length
    _PROJECT_KEY_RANDOM_SUFFIX_SIZE to the substring of size
    min(len(project_name), self._PROJECT_KEY_MAX_LENGTH_FOR_DEFAULT_GENERATOR)

    The generated project key is always size > 3. It contains at least 1 letter. All letters in the key are uppercase.
    """

    _PROJECT_KEY_MIN_LENGTH_FOR_DEFAULT_GENERATOR = 3  # project_name is at least 3 characters long
    _PROJECT_KEY_MAX_LENGTH_FOR_DEFAULT_GENERATOR = 5
    _PROJECT_KEY_MIN_NUMBER_ITERATOR = 2
    _PROJECT_KEY_MAX_NUMBER_ITERATOR = 3
    _PROJECT_KEY_RANDOM_SUFFIX_SIZE = 3
    _PROJECT_KEY_RANDOM_SUFFIX_ALPHABET = list(string.ascii_uppercase)
    _VALID_PROJECT_KEY_CHARACTERS = set(string.ascii_uppercase + string.digits)

    def __init__(self, project_name: str, existing_project_keys: Set[str]):
        self.project_name_valid_characters = self._get_project_name_with_only_valid_characters(
            project_name
        )
        self.existing_project_keys = [project_key.upper() for project_key in existing_project_keys]
        self.max_substring_length = min(
            len(project_name), self._PROJECT_KEY_MAX_LENGTH_FOR_DEFAULT_GENERATOR
        )

    def _get_project_name_with_only_valid_characters(self, project_name):
        return "".join(
            filter(
                lambda character: character in self._VALID_PROJECT_KEY_CHARACTERS,
                list(project_name.upper()),
            )
        )

    def get_default_project_key(self) -> str:
        if (
            len(self.project_name_valid_characters)
            < self._PROJECT_KEY_MIN_LENGTH_FOR_DEFAULT_GENERATOR
        ):
            return self._find_name_with_random_suffix(self.project_name_valid_characters)

        unique_key = self._find_unique_project_key("")
        if unique_key is not None:
            return unique_key

        #  if above code do not find a proper id then add incrementation
        unique_key_with_incrementation = self._find_unique_project_key_with_incrementation()
        if unique_key_with_incrementation is not None:
            return unique_key_with_incrementation

        #  if above code do not find a proper id then add random string as suffix
        possible_project_key_prefix = self.project_name_valid_characters[
            : self.max_substring_length
        ]
        return self._find_name_with_random_suffix(possible_project_key_prefix)

    def _find_name_with_random_suffix(self, possible_project_key_prefix) -> str:
        while True:
            possible_project_key = possible_project_key_prefix + self._generate_random_suffix()
            if self._is_project_key_valid(possible_project_key):
                return possible_project_key

    def _generate_random_suffix(self) -> str:
        return "".join(
            random.choices(
                self._PROJECT_KEY_RANDOM_SUFFIX_ALPHABET, k=self._PROJECT_KEY_RANDOM_SUFFIX_SIZE
            )
        )

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
            project_name_substring = (
                self.project_name_valid_characters[:sub_string_length] + additional_suffix
            )
            if self._is_project_key_valid(project_name_substring):
                return project_name_substring
        return None

    def _is_project_key_valid(self, possible_project_key: str) -> bool:
        return (
            possible_project_key not in self.existing_project_keys
            and not possible_project_key.isdigit()
        )
