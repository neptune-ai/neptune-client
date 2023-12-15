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
__all__ = ["GitRef"]

import typing

from neptune.attributes.atoms.atom import Atom
from neptune.internal.backends.api_model import (
    GitRefAttribute,
    OptionalFeatures,
)
from neptune.internal.container_type import ContainerType

if typing.TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


class GitRef(Atom):
    def _check_feature(self) -> None:
        self._container._backend.verify_feature_available(OptionalFeatures.GIT_REF)

    @staticmethod
    def getter(
        backend: "NeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
    ) -> GitRefAttribute:
        return backend.get_git_ref_attribute(container_id, container_type, path)

    def fetch(self) -> GitRefAttribute:
        self._check_feature()
        return self._backend.get_git_ref_attribute(self._container_id, self._container_type, self._path)
