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

import pytest
from mock import (
    MagicMock,
    patch,
)
from psutil import (
    AccessDenied,
    Error,
)

from neptune.envs import (
    NEPTUNE_MAX_DISK_USAGE,
    NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED,
)
from neptune.exceptions import NeptuneMaxDiskUtilizationExceeded
from neptune.internal.utils.disk_utilization import (
    NonRaisingErrorHandler,
    RaisingErrorHandler,
    ensure_disk_not_overutilize,
)


class TestDiskUtilization(unittest.TestCase):
    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    def test_handle_invalid_env_values(self):
        for value in ["True", "101", "-1"]:
            with patch.dict(os.environ, {NEPTUNE_MAX_DISK_USAGE: value}, clear=True):
                mocked_func = MagicMock()
                with warnings.catch_warnings(record=True) as warns:
                    wrapped_func = ensure_disk_not_overutilize(mocked_func)
                    wrapped_func()

                    assert len(warns) == 1
                    assert f"invalid value of '{NEPTUNE_MAX_DISK_USAGE}': '{value}" in str(warns[-1].message)
                    mocked_func.assert_called_once()

    # Catching OSError that's base error for all OS and IO errors. More info here: https://peps.python.org/pep-3151
    # Additionally, catching specific psutil's base error - psutil.Error.
    # More info about psutil.Error here: https://psutil.readthedocs.io/en/latest/index.html#psutil.Error
    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "False"})
    def test_suppressing_of_func_errors(self):
        disk_errors = [
            OSError(),
            IOError(),
            EnvironmentError(),
            UnsupportedOperation(),
            Error(),
            AccessDenied(),
        ]
        for error in disk_errors:
            mocked_func = MagicMock()
            wrapped_func = ensure_disk_not_overutilize(mocked_func)
            mocked_func.side_effect = error

            wrapped_func()  # asserting is not required as expecting that any error will be caught
            mocked_func.assert_called_once()

        non_disk_errors = [OverflowError(), AttributeError()]
        for error in non_disk_errors:
            mocked_func = MagicMock()
            wrapped_func = ensure_disk_not_overutilize(mocked_func)
            mocked_func.side_effect = error

            with self.assertRaises(BaseException):
                wrapped_func()
            mocked_func.assert_called_once()

    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_USAGE: "60"})
    @patch("psutil.disk_usage")
    def test_suppressing_of_checking_utilization_errors(self, disk_usage_mock):
        checking_errors = [
            TypeError(),
            UnsupportedOperation(),
            Error(),
            AccessDenied(),
        ]
        for error in checking_errors:
            mocked_func = MagicMock()
            wrapped_func = ensure_disk_not_overutilize(mocked_func)
            disk_usage_mock.side_effect = error

            wrapped_func()  # asserting is not required as expecting that any error will be caught
            mocked_func.assert_called_once()

    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_USAGE: "100"})
    @patch("psutil.disk_usage")
    def test_not_called_with_usage_100_percent(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 100
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)

        with pytest.raises(NeptuneMaxDiskUtilizationExceeded):
            wrapped_func()

        mocked_func.assert_not_called()

    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_USAGE: "100"})
    @patch("psutil.disk_usage")
    def test_called_when_usage_less_than_limit(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 99
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)

        wrapped_func()

        mocked_func.assert_called_once()

    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    @patch.dict(os.environ, {NEPTUNE_MAX_DISK_USAGE: "60"})
    @patch("psutil.disk_usage")
    def test_not_called_when_(self, disk_usage_mock):
        disk_usage_mock.return_value.percent = 99
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_overutilize(mocked_func)
        with pytest.raises(NeptuneMaxDiskUtilizationExceeded):
            wrapped_func()

        mocked_func.assert_not_called()


class TestDiskErrorHandler(unittest.TestCase):
    @patch("neptune.internal.utils.disk_utilization.RaisingErrorHandler")
    @patch("neptune.internal.utils.disk_utilization.NonRaisingErrorHandler")
    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "True"})
    def test_raising_handler_used_if_env_var_true(self, mock_non_raising_handler, mock_raising_handler):
        decorated = ensure_disk_not_overutilize(MagicMock())
        decorated()
        mock_raising_handler.assert_called_once()
        mock_non_raising_handler.assert_not_called()

    @patch("neptune.internal.utils.disk_utilization.RaisingErrorHandler")
    @patch("neptune.internal.utils.disk_utilization.NonRaisingErrorHandler")
    @patch.dict(os.environ, {NEPTUNE_RAISE_ERROR_ON_DISK_USAGE_EXCEEDED: "False"})
    def test_non_raising_handler_used_if_env_var_false(self, mock_non_raising_handler, mock_raising_handler):
        decorated = ensure_disk_not_overutilize(MagicMock())
        decorated()
        mock_non_raising_handler.assert_called_once()
        mock_raising_handler.assert_not_called()

    def test_non_raising_handler(self):
        func = MagicMock()
        func.side_effect = OSError

        handler = NonRaisingErrorHandler(None, func)
        handler.handle_limit_not_set()  # should not raise exception

        handler = NonRaisingErrorHandler(90.0, func)
        handler.handle_utilization_calculation_error()  # should not raise exception

        handler.handle_limit_not_exceeded()  # should not raise exception

        handler.handle_limit_exceeded(100)  # should not raise exception

        handler.run()  # should not raise exception

    def test_raising_handler(self):
        func = MagicMock()
        func.side_effect = OSError

        with pytest.raises(OSError):
            handler = RaisingErrorHandler(None, func)
            handler.handle_limit_not_set()

        with pytest.raises(OSError):
            handler = RaisingErrorHandler(None, func)
            handler.handle_utilization_calculation_error()

        with pytest.raises(OSError):
            handler = RaisingErrorHandler(100.0, func)
            handler.handle_limit_not_exceeded()

        with pytest.raises(NeptuneMaxDiskUtilizationExceeded):
            handler = RaisingErrorHandler(90.0, func)
            handler.handle_limit_exceeded(100)

        with pytest.raises(OSError):
            handler.run()

        func.side_effect = None
        with patch("neptune.internal.utils.disk_utilization.get_disk_utilization_percent", return_value=95):
            with pytest.raises(NeptuneMaxDiskUtilizationExceeded):
                handler.run()
