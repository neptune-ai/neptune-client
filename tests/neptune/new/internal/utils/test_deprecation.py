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

import pytest

from neptune.new.exceptions import NeptuneParametersCollision
from neptune.new.internal.utils.deprecation import deprecated, deprecated_parameter


@deprecated_parameter(deprecated_kwarg_name="deprecated_param", required_kwarg_name="new_param")
def fun_with_deprecated_param(*, new_param):
    return new_param


@deprecated()
def deprecated_func():
    pass


@deprecated(alternative="non_deprecated_func")
def deprecated_func_with_alternative():
    pass


class TestDeprecatedParameter(unittest.TestCase):
    @mock.patch("neptune.new.internal.utils.deprecation.logger")
    def test_deprecated_not_used(self, logger_mock):
        self.assertEqual(42, fun_with_deprecated_param(new_param=42))
        self.assertFalse(logger_mock.called)

    @mock.patch("neptune.new.internal.utils.deprecation.logger")
    def test_deprecated_replaced(self, logger_mock):
        # pylint: disable=unexpected-keyword-arg
        # pylint: disable=missing-kwoa
        self.assertEqual(42, fun_with_deprecated_param(deprecated_param=42))
        self.assertEqual(1, logger_mock.warning.call_count)
        self.assertEqual(
            call(
                "Parameter `%s` is deprecated, use `%s` instead."
                " We'll end support of it in `neptune-client==1.0.0`.",
                "deprecated_param",
                "new_param",
            ),
            logger_mock.warning.call_args,
        )

    def test_conflict(self):
        with self.assertRaises(NeptuneParametersCollision):
            # pylint: disable=unexpected-keyword-arg
            fun_with_deprecated_param(new_param=42, deprecated_param=42)

    def test_passing_deprecated_parameter_as_none(self):
        # pylint: disable=unexpected-keyword-arg
        # pylint: disable=missing-kwoa
        self.assertIsNone(fun_with_deprecated_param(deprecated_param=None))

        # test collision
        with self.assertRaises(NeptuneParametersCollision):
            fun_with_deprecated_param(new_param=None, deprecated_param=None)

    def test_deprecated_func_without_alternative(self):
        with pytest.deprecated_call(
            match="`deprecated_func` is deprecated and will be removed. We'll end support of "
            "it in `neptune-client==1.0.0`."
        ):
            deprecated_func()

    def test_deprecated_func_with_alternative(self):
        with pytest.deprecated_call(
            match="`deprecated_func_with_alternative` is deprecated, "
            "use `non_deprecated_func` instead. We'll end support of it in "
            "`neptune-client==1.0.0`."
        ):
            deprecated_func_with_alternative()
