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

from __future__ import annotations

__all__ = [
    "NQLQuery",
    "NQLEmptyQuery",
    "NQLAggregator",
    "NQLQueryAggregate",
    "NQLAttributeOperator",
    "NQLAttributeType",
    "NQLQueryAttribute",
    "RawNQLQuery",
]

import typing
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


@dataclass
class NQLQuery:
    def eval(self) -> NQLQuery:
        return self


@dataclass
class NQLEmptyQuery(NQLQuery):
    def __str__(self) -> str:
        return ""


class NQLAggregator(str, Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class NQLQueryAggregate(NQLQuery):
    items: Iterable[NQLQuery]
    aggregator: NQLAggregator

    def eval(self) -> NQLQuery:
        self.items = list(filter(lambda nql: not isinstance(nql, NQLEmptyQuery), (item.eval() for item in self.items)))

        if len(self.items) == 0:
            return NQLEmptyQuery()
        elif len(self.items) == 1:
            return self.items[0]
        return self

    def __str__(self) -> str:
        evaluated = self.eval()
        if isinstance(evaluated, NQLQueryAggregate):
            return "(" + f" {self.aggregator.value} ".join(map(str, self.items)) + ")"
        return str(evaluated)


class NQLAttributeOperator(str, Enum):
    EQUALS = "="
    CONTAINS = "CONTAINS"
    GREATER_THAN = ">"


class NQLAttributeType(str, Enum):
    STRING = "string"
    STRING_SET = "stringSet"
    EXPERIMENT_STATE = "experimentState"
    BOOLEAN = "bool"
    DATETIME = "datetime"
    INTEGER = "integer"
    FLOAT = "float"


@dataclass
class NQLQueryAttribute(NQLQuery):
    name: str
    type: NQLAttributeType
    operator: NQLAttributeOperator
    value: typing.Union[str, bool]

    def __str__(self) -> str:
        if isinstance(self.value, bool):
            value = str(self.value).lower()
        else:
            value = f'"{self.value}"'

        return f"(`{self.name}`:{self.type.value} {self.operator.value} {value})"


@dataclass
class RawNQLQuery(NQLQuery):
    query: str

    def eval(self) -> NQLQuery:
        if self.query == "":
            return NQLEmptyQuery()
        return self

    def __str__(self) -> str:
        evaluated = self.eval()
        if isinstance(evaluated, RawNQLQuery):
            return self.query
        return str(evaluated)
