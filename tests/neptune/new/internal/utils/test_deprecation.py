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
import unittest
from unittest import mock
from unittest.mock import call

from neptune.new.exceptions import NeptuneParametersCollision
from neptune.new.internal.utils.deprecation import deprecated_parameter


@deprecated_parameter(deprecated_kwarg_name="deprecated_param", required_kwarg_name="new_param")
def fun(*, new_param):
    return new_param


class TestDeprecatedParameter(unittest.TestCase):
    @mock.patch("neptune.new.internal.utils.deprecation.logger")
    def test_deprecated_not_used(self, logger_mock):
        self.assertEqual(42, fun(new_param=42))
        self.assertFalse(logger_mock.called)

    @mock.patch("neptune.new.internal.utils.deprecation.logger")
    def test_deprecated_replaced(self, logger_mock):
        # pylint: disable=unexpected-keyword-arg
        # pylint: disable=missing-kwoa
        self.assertEqual(42, fun(deprecated_param=42))
        self.assertEqual(1, logger_mock.warning.call_count)
        self.assertEqual(
            call(
                "parameter `%s` is deprecated, use `%s` instead."
                " We'll end support of it in `neptune-client==1.0.0`.",
                "deprecated_param",
                "new_param",
            ),
            logger_mock.warning.call_args,
        )

    def test_conflict(self):
        with self.assertRaises(NeptuneParametersCollision):
            # pylint: disable=unexpected-keyword-arg
            fun(new_param=42, deprecated_param=42)

    def test_passing_deprecated_parameter_as_none(self):
        # pylint: disable=unexpected-keyword-arg
        # pylint: disable=missing-kwoa
        self.assertIsNone(fun(deprecated_param=None))

        # test collision
        with self.assertRaises(NeptuneParametersCollision):
            fun(new_param=None, deprecated_param=None)
