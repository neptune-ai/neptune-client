#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = [
    "NQLQuery",
    "NQLAggregator",
    "NQLQueryAggregate",
    "NQLAttributeOperator",
    "NQLAttributeType",
    "NQLQueryAttribute",
]

from enum import Enum
from typing import Iterable
from dataclasses import dataclass


@dataclass
class NQLQuery:
    pass


class NQLAggregator(str, Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class NQLQueryAggregate(NQLQuery):
    items: Iterable[NQLQuery]
    aggregator: NQLAggregator

    def __str__(self) -> str:
        if self.items:
            return "(" + f" {self.aggregator.value} ".join(map(str, self.items)) + ")"
        return ""


class NQLAttributeOperator(str, Enum):
    EQUALS = "="
    CONTAINS = "CONTAINS"


class NQLAttributeType(str, Enum):
    STRING = "string"
    STRING_SET = "stringSet"
    EXPERIMENT_STATE = "experimentState"


@dataclass
class NQLQueryAttribute(NQLQuery):
    name: str
    type: NQLAttributeType
    operator: NQLAttributeOperator
    value: str

    def __str__(self) -> str:
        return f'(`{self.name}`:{self.type.value} {self.operator.value} "{self.value}")'
