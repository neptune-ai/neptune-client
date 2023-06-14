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
__all__ = [
    "NeptuneException",
    "IncorrectIdentifier",
    "ObjectNotFound",
    "ProjectKeyCollision",
    "ProjectKeyInvalid",
    "ProjectNameCollision",
    "ProjectNameInvalid",
    "ProjectPrivacyRestricted",
    "ProjectsLimitReached",
    "ActiveProjectsLimitReached",
]

from abc import ABCMeta
from dataclasses import dataclass


@dataclass(frozen=True)
class NeptuneException(Exception, metaclass=ABCMeta):
    ...


@dataclass(frozen=True)
class IncorrectIdentifier(NeptuneException):
    identifier: str


@dataclass(frozen=True)
class ObjectNotFound(NeptuneException):
    ...


@dataclass(frozen=True)
class ProjectKeyCollision(NeptuneException):
    key: str


@dataclass(frozen=True)
class ProjectKeyInvalid(NeptuneException):
    key: str
    reason: str


@dataclass(frozen=True)
class ProjectNameCollision(NeptuneException):
    key: str


@dataclass(frozen=True)
class ProjectNameInvalid(NeptuneException):
    name: str


@dataclass(frozen=True)
class ProjectPrivacyRestricted(NeptuneException):
    requested: str
    allowed: str


@dataclass(frozen=True)
class ProjectsLimitReached(NeptuneException):
    ...


@dataclass(frozen=True)
class ActiveProjectsLimitReached(NeptuneException):
    current_quota: str
