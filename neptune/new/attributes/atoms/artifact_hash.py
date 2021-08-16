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
from typing import Union


from neptune.new.internal.operation import AssignArtifactHash
from neptune.new.types.atoms.artifact_hash import ArtifactHash as ArtifactHashVal
from neptune.new.attributes.atoms.atom import Atom


class ArtifactHash(Atom):

    def assign(self, value: Union[ArtifactHashVal, str], wait: bool = False):
        if not isinstance(value, ArtifactHashVal):
            value = ArtifactHashVal(value)

        with self._run.lock():
            self._enqueue_operation(AssignArtifactHash(self._path, value.value), wait)

    def fetch(self) -> str:
        # pylint: disable=protected-access
        val = self._backend.get_string_attribute(self._run_uuid, self._path)
        return val.value
