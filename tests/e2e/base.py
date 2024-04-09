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
__all__ = ["BaseE2ETest", "AVAILABLE_CONTAINERS", "fake", "Parameters", "ParametersFactory"]

import inspect
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)

import pytest
from faker import Faker

ParameterSet = Any

fake = Faker()

AVAILABLE_CONTAINERS = [
    pytest.param("run"),
    pytest.param("project", marks=pytest.mark.skip(reason="Project not supported")),
    pytest.param(
        "model",
        marks=pytest.mark.skip(reason="Model not implemented"),
    ),
    pytest.param(
        "model_version",
        marks=pytest.mark.skip(reason="Model not implemented"),
    ),
]


class Parameters:
    param_d: Dict[str, Any]

    def __init__(self, params: List[ParameterSet]) -> "Parameters":
        self.param_d = {p.values[0]: p for p in params}

    def _modify(self, *args: Union[str, str], func: Callable[[str], ParameterSet]) -> "Parameters":
        for arg in args:
            self.param_d[arg] = func(arg)
        return self

    def skip(self, *args: str, reason: Optional[str] = None) -> "Parameters":
        return self._modify(*args, func=lambda x: pytest.param(x, marks=pytest.mark.skip(reason=reason)))

    def xfail(
        self, *args: str, reason: Optional[str] = None, strict=True, raises: Optional[Exception] = None
    ) -> "Parameters":
        return self._modify(
            *args, func=lambda x: pytest.param(x, marks=pytest.mark.xfail(reason=reason, strict=strict, raises=raises))
        )

    def run(self, *args: str) -> "Parameters":
        return self._modify(*args, func=lambda x: pytest.param(x))

    def eval(self) -> List[ParameterSet]:
        return list(self.param_d.values())


class ParametersFactory:

    @staticmethod
    def custom(params: List[Union[str, ParameterSet]]) -> Parameters:
        normalized = []
        for p in params:
            if isinstance(p, str):
                normalized.append(pytest.param(p))
            else:
                normalized.append(p)
        return Parameters(normalized)

    @staticmethod
    def available_containers() -> Parameters:
        return Parameters(AVAILABLE_CONTAINERS.copy())


class BaseE2ETest:
    def gen_key(self):
        # Get test name
        caller_name = inspect.stack()[1][3]
        return f"{self.__class__.__name__}/{caller_name}/{fake.unique.slug()}"
