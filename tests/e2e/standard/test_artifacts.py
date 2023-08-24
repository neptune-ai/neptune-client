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
import os
import tempfile
from pathlib import (
    Path,
    PurePosixPath,
)

import pytest

from neptune.metadata_containers import MetadataContainer
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)
from tests.e2e.utils import (
    tmp_context,
    with_check_if_file_appears,
)


class TestArtifacts(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_local_creation(self, container: MetadataContainer):
        first, second = self.gen_key(), self.gen_key()
        filename = fake.unique.file_name()

        with tmp_context() as tmp:
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            container[first].track_files(".")
            container[second].track_files(f"file://{tmp}")

            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_assignment(self, container: MetadataContainer):
        first, second = self.gen_key(), self.gen_key()
        filename = fake.unique.file_name()

        with tmp_context():
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            container[first].track_files(filename)
            container.wait()
            container[second] = container[first].fetch()
            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_local_download(self, container: MetadataContainer):
        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.unique.file_name(), fake.unique.file_path(depth=3, absolute=False)

        with tmp_context() as tmp:
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            # Relative path
            container[first].track_files(filename)
            # Absolute path
            container[second].track_files(f"file://{tmp}")

            container.sync()

            with tmp_context():
                with with_check_if_file_appears(Path(f"artifacts/{filename}")):
                    container[first].download("artifacts/")

                with with_check_if_file_appears(Path(filepath)):
                    container[second].download()

    @pytest.mark.s3
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_s3_creation(self, container: MetadataContainer, bucket, environment):
        first, second, prefix = (
            self.gen_key(),
            self.gen_key(),
            f"{environment.project}/{self.gen_key()}/{type(container).__name__}",
        )
        filename = fake.unique.file_name()

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, f"{prefix}/{filename}")

        container[first].track_files(f"s3://{bucket_name}/{prefix}/{filename}")
        container[second].track_files(f"s3://{bucket_name}/{prefix}")

        container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.s3
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_s3_download(self, container: MetadataContainer, bucket, environment):
        first = self.gen_key()
        prefix = f"{environment.project}/{self.gen_key()}/{type(container).__name__}"
        filename, filepath = fake.unique.file_name(), fake.unique.file_path(depth=3, absolute=False)

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, f"{prefix}/{filename}")
            s3_client.meta.client.upload_file(filepath, bucket_name, f"{prefix}/{filepath}")

        container[first].track_files(f"s3://{bucket_name}/{prefix}")

        container.sync()

        with tempfile.TemporaryDirectory() as tmp:
            with with_check_if_file_appears(f"{tmp}/{filename}"):
                container[first].download(tmp)

        with tmp_context():
            with with_check_if_file_appears(filename):
                container[first].download()

    @pytest.mark.s3
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_s3_existing(self, container: MetadataContainer, bucket, environment):
        first, second, prefix = (
            self.gen_key(),
            self.gen_key(),
            f"{environment.project}/{self.gen_key()}/{type(container).__name__}",
        )
        filename, filepath = fake.file_name(), fake.file_path(depth=3, absolute=False)

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, f"{prefix}/{filename}")
            s3_client.meta.client.upload_file(filepath, bucket_name, f"{prefix}/{filepath}")

        # Track all files - "a" and "b" to first artifact
        container[first].track_files(f"s3://{bucket_name}/{prefix}/")

        # Track only the "a" file to second artifact
        container[second].track_files(f"s3://{bucket_name}/{prefix}/{filename}")
        container.sync()

        # Add "b" file to existing second artifact
        # so it should be now identical as first
        container[second].track_files(
            f"s3://{bucket_name}/{prefix}/{filepath}",
            destination=str(PurePosixPath(filepath).parent),
        )
        container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_local_existing(self, container: MetadataContainer):
        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3, absolute=False)

        with tmp_context() as tmp:
            with open(filename, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            # Track all files - "a" and "b" to first artifact
            container[first].track_files(".")

            # Track only the "a" file to second artifact
            container[second].track_files(f"file://{Path(tmp)}/{filename}")
            container.sync()

            # Add "b" file to existing second artifact
            # so it should be now identical as first
            container[second].track_files(filepath, destination=str(Path(filepath).parent))
            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()
