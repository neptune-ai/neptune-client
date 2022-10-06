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
import pathlib
import typing

from neptune.new.attributes.atoms.atom import Atom
from neptune.new.internal.artifacts.types import (
    ArtifactDriver,
    ArtifactDriversMap,
    ArtifactFileData,
)
from neptune.new.internal.backends.api_model import OptionalFeatures
from neptune.new.internal.operation import (
    AssignArtifact,
    TrackFilesToArtifact,
)
from neptune.new.types.atoms.artifact import Artifact as ArtifactVal


class Artifact(Atom):
    def _check_feature(self):
        # pylint: disable=protected-access
        self._container._backend.verify_feature_available(OptionalFeatures.ARTIFACTS)

    def assign(self, value: ArtifactVal, wait: bool = False):
        self._check_feature()
        # this function should be used only with ArtifactVal
        if not isinstance(value, ArtifactVal):
            raise TypeError("Value of unsupported type {}".format(type(value)))

        with self._container.lock():
            self._enqueue_operation(AssignArtifact(self._path, value.hash), wait)

    def fetch(self) -> ArtifactVal:
        self._check_feature()
        return ArtifactVal(self.fetch_hash())

    def fetch_hash(self) -> str:
        self._check_feature()
        val = self._backend.get_artifact_attribute(self._container_id, self._container_type, self._path)
        return val.hash

    def fetch_files_list(self) -> typing.List[ArtifactFileData]:
        self._check_feature()
        artifact_hash = self.fetch_hash()
        return self._backend.list_artifact_files(
            self._container._project_id,  # pylint: disable=protected-access
            artifact_hash,
        )

    def download(self, destination: str = None):
        self._check_feature()
        for file_definition in self.fetch_files_list():
            driver: typing.Type[ArtifactDriver] = ArtifactDriversMap.match_type(file_definition.type)
            file_destination = pathlib.Path(destination or ".") / pathlib.Path(file_definition.file_path)
            file_destination.parent.mkdir(parents=True, exist_ok=True)
            driver.download_file(file_destination, file_definition)

    def track_files(self, path: str, destination: str = None, wait: bool = False):
        self._check_feature()
        with self._container.lock():
            self._enqueue_operation(
                TrackFilesToArtifact(
                    self._path,
                    self._container._project_id,  # pylint: disable=protected-access
                    [(path, destination)],
                ),
                wait,
            )
