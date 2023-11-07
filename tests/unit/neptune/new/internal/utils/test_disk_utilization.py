#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
import unittest
import warnings
from io import UnsupportedOperation

from mock import (
    MagicMock,
    patch,
)
from psutil import (
    AccessDenied,
    Error,
)

from neptune.envs import (
    NEPTUNE_MAX_DISK_UTILIZATION,
    NEPTUNE_NON_RAISING_ON_DISK_ISSUE,
)
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize


class TestDiskUtilization(unittest.TestCase):
    @patch.dict(os.environ, {NEPTUNE_NON_RAISING_ON_DISK_ISSUE: "True"})
    def test_handle_invalid_env_values(self):
        for value in ["True", "101", "-1"]:
            with patch.dict(os.environ, {NEPTUNE_MAX_DISK_UTILIZATION: value}, clear=True):
                mocked_func = MagicMock()
                with warnings.catch_warnings(record=True) as warns:
                    wrapped_func = ensure_disk_not_overutilize(mocked_func)
                    wrapped_func()

                    assert len(warns) == 1
                    assert f"invalid value of '{NEPTUNE_MAX_DISK_UTILIZATION}': '{value}" in str(warns[-1].message)
                    mocked_func.assert_called_once()

    # Catching OSError that's base error for all OS and IO errors. More info here: https://peps.python.org/pep-3151
    # Additionally, catching specific psutil's base error - psutil.Error.
    # More info about psutil.Error here: https://psutil.readthedocs.io/en/latest/index.html#psutil.Error
    @patch.dict(os.environ, {NEPTUNE_NON_RAISING_ON_DISK_ISSUE: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_UTILIZATION: "60"})
    @patch("psutil.disk_usage")
    def test_suppressing_of_env_errors(self, disk_usage_mock):
        env_errors = [
            TypeError(),
            OSError(),
            IOError(),
            EnvironmentError(),
            UnsupportedOperation(),
            Error(),
            AccessDenied(),
        ]
        for error in env_errors:
            mocked_func = MagicMock()
            wrapped_func = ensure_disk_not_overutilize(mocked_func)
            disk_usage_mock.side_effect = error

            wrapped_func()  # asserting is not required as expecting that any error will be caught
            mocked_func.assert_not_called()

        non_env_errors = [OverflowError(), AttributeError()]
        for error in non_env_errors:
            mocked_func = MagicMock()
            wrapped_func = ensure_disk_not_overutilize(mocked_func)
            disk_usage_mock.side_effect = error

            with self.assertRaises(BaseException):
                wrapped_func()
            mocked_func.assert_not_called()

    @patch.dict(os.environ, {NEPTUNE_NON_RAISING_ON_DISK_ISSUE: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_UTILIZATION: "100"})
    @patch("psutil.disk_usage")
    def test_not_called_with_usage_100_percent(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 100
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)

        wrapped_func()

        mocked_func.assert_not_called()

    @patch.dict(os.environ, {NEPTUNE_NON_RAISING_ON_DISK_ISSUE: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_UTILIZATION: "100"})
    @patch("psutil.disk_usage")
    def test_called_when_usage_less_than_limit(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 99
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)

        wrapped_func()

        mocked_func.assert_called_once()

    @patch.dict(os.environ, {NEPTUNE_NON_RAISING_ON_DISK_ISSUE: "False"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_UTILIZATION: "60"})
    @patch("psutil.disk_usage")
    def test_not_called_when_(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 99
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)

        wrapped_func()

        mocked_func.assert_called_once()
