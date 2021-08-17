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
import pathlib
import typing

from neptune.new.internal.operation import TrackFilesToNewArtifact
from neptune.new.internal.artifacts.types import ArtifactFileData, ArtifactDriversMap, ArtifactDriver
from neptune.new.attributes.atoms.atom import Atom


class Artifact(Atom):
    def fetch_hash(self) -> str:
        val = self._backend.get_artifact_attribute(self._run_uuid, self._path)
        return val.hash

    def fetch_files_list(self) -> typing.List[ArtifactFileData]:
        artifact_hash = self.fetch_hash()
        return self._backend.list_artifact_files(
            self._run._project_uuid, artifact_hash  # pylint: disable=protected-access
        )

    def download(self, destination: str = None):
        for file_definition in self.fetch_files_list():
            driver: typing.Type[ArtifactDriver] = ArtifactDriversMap.match_type(file_definition.type)
            file_destination = pathlib.Path(destination) / file_definition.file_path
            file_destination.parent.mkdir(parents=True, exist_ok=True)
            driver.download_file(file_destination, file_definition)

    def track_files_to_new(self, source_location: str, wait: bool = False):
        print('Track files to new', source_location)
        with self._run.lock():
            self._enqueue_operation(TrackFilesToNewArtifact(self._path, source_location), wait)

    def track_files_to_existing(self, path: str):
        # FIXME: implement in NPT-10545
        raise NotImplementedError
