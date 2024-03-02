#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import pytest

from neptune.exceptions import (
    NeptuneIntegrationNotInstalledException,
    NeptuneMissingRequirementException,
)
from neptune.utils import (
    is_installed,
    require_installed,
)


def assert_exception_message_contains(exc_message: str, *substrings: str) -> None:
    for substring in substrings:
        assert substring in exc_message


def test_is_installed_if_package_installed():
    assert is_installed("neptune")


def test_is_installed_if_package_not_installed():
    assert not is_installed("wrong-package-name")


def test_check_requirement_if_package_installed():
    require_installed("neptune")
    assert True


def test_check_requirement_if_package_not_installed():
    with pytest.raises(NeptuneMissingRequirementException) as e:
        require_installed("wrong-package-name")

    assert_exception_message_contains(
        str(e.value),
        "NeptuneMissingRequirementException",
        "wrong-package-name",
        "pip install wrong-package-name",
        'pip install "neptune[wrong-package-name]"',
    )


def test_check_requirement_if_integration_not_installed():
    with pytest.raises(NeptuneIntegrationNotInstalledException) as e:
        require_installed(
            "neptune-wrong-integration",
            suggestion="wrong-integration",
            exception=NeptuneIntegrationNotInstalledException,
        )

    assert_exception_message_contains(
        str(e.value),
        "NeptuneIntegrationNotInstalledException",
        "neptune-wrong-integration",
        "pip install neptune-wrong-integration",
        'pip install "neptune[wrong-integration]"',
    )
