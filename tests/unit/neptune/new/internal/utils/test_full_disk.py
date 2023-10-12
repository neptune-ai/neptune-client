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
import unittest
from io import UnsupportedOperation

from mock import (
    MagicMock,
    patch,
)
from psutil import (
    AccessDenied,
    Error,
)

from neptune.internal.utils.disk_full import ensure_disk_not_full


@ensure_disk_not_full(True, 42)
def dummy_test_func():
    pass


class TestDiskFull(unittest.TestCase):

    # Catching OSError that's base error for all OS and IO errors. More info here: https://peps.python.org/pep-3151
    # Additionally, catching specific psutil's base error - psutil.Error.
    # More info about psutil.Error here: https://psutil.readthedocs.io/en/latest/index.html#psutil.Error
    @patch("neptune.internal.utils.disk_full.get_disk_utilization_percent")
    def test_suppressing_of_env_errors(self, get_disk_utilization_percent):
        env_errors = [OSError(), IOError(), EnvironmentError(), UnsupportedOperation(), Error(), AccessDenied()]

        for error in env_errors:
            get_disk_utilization_percent.side_effect = error
            dummy_test_func()  # asserting is not required as expecting that any error will be caught

        non_env_errors = [ValueError(), OverflowError()]
        for error in non_env_errors:
            get_disk_utilization_percent.side_effect = error
            with self.assertRaises(BaseException):
                dummy_test_func()

    @patch("neptune.internal.utils.disk_full.get_disk_utilization_percent")
    def test_not_called_with_usage_100_percent(self, get_disk_utilization_percent):
        get_disk_utilization_percent.return_value = 100
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_full(True, 100)(mocked_func)

        wrapped_func()

        mocked_func.assert_not_called()

    @patch("neptune.internal.utils.disk_full.get_disk_utilization_percent")
    def test_called_when_usage_less_than_limit(self, get_disk_utilization_percent):
        get_disk_utilization_percent.return_value = 99
        mocked_func = MagicMock()
        wrapped_func = ensure_disk_not_full(True, 100)(mocked_func)

        wrapped_func()

        mocked_func.assert_called_once()
