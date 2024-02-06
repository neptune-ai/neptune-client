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
import warnings
from unittest.mock import (
    ANY,
    patch,
)

import pytest

from neptune.common.warnings import (
    NeptuneDeprecationWarning,
    warn_once,
)
from neptune.exceptions import NeptuneParametersCollision
from neptune.internal.utils.deprecation import (
    deprecated,
    deprecated_parameter,
)


@deprecated_parameter(deprecated_kwarg_name="deprecated_param", required_kwarg_name="new_param")
def fun_with_deprecated_param(*, new_param):
    return new_param


@deprecated()
def deprecated_func():
    pass


@deprecated(alternative="non_deprecated_func")
def deprecated_func_with_alternative():
    pass


class TestDeprecatedParameter:
    def test_deprecated_not_used(self):
        # https://stackoverflow.com/questions/45671803/how-to-use-pytest-to-assert-no-warning-is-raised
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            fun_with_deprecated_param(new_param=42)

    def test_deprecated_replaced(self):
        with pytest.deprecated_call(
            match="Parameter `deprecated_param` is deprecated, use `new_param` instead. We'll end support of it in "
            "next major release."
        ):
            value = fun_with_deprecated_param(deprecated_param=42)
            assert value == 42

    def test_conflict(self):
        with pytest.raises(NeptuneParametersCollision):
            fun_with_deprecated_param(new_param=42, deprecated_param=42)

    def test_passing_deprecated_parameter_as_none(self):
        assert fun_with_deprecated_param(deprecated_param=None) is None

        with pytest.raises(NeptuneParametersCollision):
            value = fun_with_deprecated_param(new_param=None, deprecated_param=None)
            assert value is None

    def test_deprecated_func_without_alternative(self):
        with pytest.deprecated_call(
            match="`deprecated_func` is deprecated and will be removed. We'll end support of "
            "it in next major release."
        ):
            deprecated_func()

    def test_deprecated_func_with_alternative(self):
        with pytest.deprecated_call(
            match="`deprecated_func_with_alternative` is deprecated, "
            "use `non_deprecated_func` instead. We'll end support of it in "
            "next major release."
        ):
            deprecated_func_with_alternative()

    @patch("warnings.warn")
    def test_warn_once(self, warn):
        warn_once(message="Deprecation message 1")
        warn_once(message="Deprecation message 1")

        warn.assert_called_once_with(
            message="Deprecation message 1", category=NeptuneDeprecationWarning, stacklevel=ANY
        )
