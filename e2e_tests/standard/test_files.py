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
import os
import random
import uuid
from itertools import product
from typing import Set
from zipfile import ZipFile

import pytest

from e2e_tests.base import BaseE2ETest, AVAILABLE_CONTAINERS, fake
from e2e_tests.utils import tmp_context
from neptune.new.metadata_containers import MetadataContainer
from neptune.new.internal.backends.api_model import MultipartConfig, OptionalFeatures
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.types import FileSet


class TestUpload(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_using_new_api(self, container: MetadataContainer):
        # pylint: disable=protected-access
        assert isinstance(container._backend, HostedNeptuneBackend)
        assert container._backend._client_config.has_feature(
            OptionalFeatures.MULTIPART_UPLOAD
        )
        assert isinstance(
            container._backend._client_config.multipart_config, MultipartConfig
        )

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    @pytest.mark.parametrize(
        "file_size",
        [
            pytest.param(10 * 2 ** 20, id="big"),  # 10 MB, multipart
            pytest.param(100 * 2 ** 10, id="small"),  # 100 kB, single upload
        ],
    )
    def test_single_file(self, container: MetadataContainer, file_size: int):
        key = self.gen_key()
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        with tmp_context():
            # create file_size file
            with open(filename, "wb") as file:
                file.write(b"\0" * file_size)
            container[key].upload(filename)

            container.sync()
            container[key].download(downloaded_filename)

            assert os.path.getsize(downloaded_filename) == file_size
            with open(downloaded_filename, "rb") as file:
                content = file.read()
                assert len(content) == file_size
                assert content == b"\0" * file_size

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_fileset(self, container: MetadataContainer):
        key = self.gen_key()
        large_filesize = 10 * 2 ** 20  # 10MB
        large_filename = fake.file_name()
        small_files = [
            (f"{uuid.uuid4()}.{fake.file_extension()}", fake.sentence().encode("utf-8"))
            for _ in range(100)
        ]

        with tmp_context():
            # create single large file (multipart) and a lot of very small files
            with open(large_filename, "wb") as file:
                file.write(b"\0" * large_filesize)
            for filename, contents in small_files:
                with open(filename, "wb") as file:
                    file.write(contents)

            small_filenames = [filename for filename, _ in small_files]
            # make sure there are no duplicates
            assert len({large_filename, *small_filenames}) == len(small_files) + 1

            # when one file as fileset uploaded
            container[key].upload_files([large_filename])

            # then check if will be downloaded
            container.sync()
            container[key].download("downloaded1.zip")

            with ZipFile("downloaded1.zip") as zipped:
                assert set(zipped.namelist()) == {large_filename, "/"}
                with zipped.open(large_filename, "r") as file:
                    content = file.read()
                    assert len(content) == large_filesize
                    assert content == b"\0" * large_filesize

            # when small files as fileset uploaded
            container[key].upload_files(small_filenames)

            # then check if everything will be downloaded
            container.sync()
            container[key].download("downloaded2.zip")

            with ZipFile("downloaded2.zip") as zipped:
                assert set(zipped.namelist()) == {large_filename, "/", *small_filenames}
                with zipped.open(large_filename, "r") as file:
                    content = file.read()
                    assert len(content) == large_filesize
                    assert content == b"\0" * large_filesize
                for filename, expected_content in small_files:
                    with zipped.open(filename, "r") as file:
                        content = file.read()
                        assert len(content) == len(expected_content)
                        assert content == expected_content

            # when first file is removed
            container[key].delete_files([large_filename])

            # then check if the rest will be downloaded
            container.sync()
            container[key].download("downloaded3.zip")

            with ZipFile("downloaded3.zip") as zipped:
                assert set(zipped.namelist()) == {"/", *small_filenames}
                for filename, expected_content in small_files:
                    with zipped.open(filename, "r") as file:
                        content = file.read()
                        assert len(content) == len(expected_content)
                        assert content == expected_content

    @classmethod
    def _gen_tree_paths(cls, depth, width=3) -> Set:
        """Generates all subdirectories of some random tree directory structure"""
        this_level_dirs = (fake.word() + "/" for _ in range(width))
        if depth == 1:
            return set(this_level_dirs)
        else:
            subpaths = cls._gen_tree_paths(depth=depth - 1, width=width)
            new_paths = set(
                "".join(prod) for prod in product(subpaths, this_level_dirs)
            )
            subpaths.update(new_paths)
            return subpaths

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_fileset_nested_structure(self, container: MetadataContainer):
        key = self.gen_key()
        possible_paths = self._gen_tree_paths(depth=3)

        small_files = [
            (
                f"{path}{uuid.uuid4()}.{fake.file_extension()}",
                os.urandom(random.randint(10 ** 3, 10 ** 6)),
            )
            for path in possible_paths
        ]

        with tmp_context():
            # create dirs
            for dir_path in possible_paths:
                os.makedirs(dir_path, exist_ok=True)
            # create  a lot of very small files in different directories
            for filename, contents in small_files:
                with open(filename, "wb") as file:
                    file.write(contents)

            small_filenames = [filename for filename, _ in small_files]
            # make sure there are no duplicates
            assert len({*small_filenames}) == len(small_files)

            # when small files as fileset uploaded
            container[key].upload_files(".")

            # then check if everything will be downloaded
            container.sync()
            container[key].download("downloaded.zip")

            with ZipFile("downloaded.zip") as zipped:
                assert set(zipped.namelist()) == {
                    "/",
                    *possible_paths,
                    *small_filenames,
                }
                for filename, expected_content in small_files:
                    with zipped.open(filename, "r") as file:
                        content = file.read()
                        assert len(content) == len(expected_content)
                        assert content == expected_content

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_reset_fileset(self, container: MetadataContainer):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()
        content1 = os.urandom(random.randint(10 ** 3, 10 ** 6))
        content2 = os.urandom(random.randint(10 ** 3, 10 ** 6))

        with tmp_context():
            # create file1 and file2
            with open(filename1, "wb") as file1, open(filename2, "wb") as file2:
                file1.write(content1)
                file2.write(content2)

            # upload file1 to initial fileset
            container[key].upload_files(filename1)

            # then replace [file1] set with [file2] to the same key
            container.sync()
            container[key] = FileSet([filename2])

            # check if there's content of SECOND uploaded file
            container.sync()
            container[key].download("downloaded.zip")
            with ZipFile("downloaded.zip") as zipped:
                assert set(zipped.namelist()) == {filename2, "/"}
                with zipped.open(filename2, "r") as file:
                    content = file.read()
                    assert len(content) == len(content2)
                    assert content == content2

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    @pytest.mark.parametrize("delete_attribute", [True, False])
    def test_single_file_override(
        self, container: MetadataContainer, delete_attribute: bool
    ):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()
        content1 = os.urandom(random.randint(10 ** 3, 10 ** 6))
        content2 = os.urandom(random.randint(10 ** 3, 10 ** 6))
        downloaded_filename = fake.file_name()

        with tmp_context():
            # create file1 and file2
            with open(filename1, "wb") as file1, open(filename2, "wb") as file2:
                file1.write(content1)
                file2.write(content2)

            # upload file1 to key
            container[key].upload(filename1)

            if delete_attribute:
                # delete attribute
                del container[key]
                # make sure that attribute does not exist
                container.sync()
                with pytest.raises(AttributeError):
                    container[key].download(downloaded_filename)

            # then upload file2 to the same key
            container[key].upload(filename2)

            # check if there's content of SECOND uploaded file
            container.sync()
            container[key].download(downloaded_filename)
            with open(downloaded_filename, "rb") as file:
                content = file.read()
                assert len(content) == len(content2)
                assert content == content2

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    @pytest.mark.parametrize("delete_attribute", [True, False])
    def test_fileset_file_override(
        self, container: MetadataContainer, delete_attribute: bool
    ):
        key = self.gen_key()
        filename = fake.file_name()
        content1 = os.urandom(random.randint(10 ** 3, 10 ** 6))
        content2 = os.urandom(random.randint(10 ** 3, 10 ** 6))

        with tmp_context():
            # create file
            with open(filename, "wb") as file1:
                file1.write(content1)
            # upload file1 to key
            container[key].upload_files([filename])

            if delete_attribute:
                # delete attribute
                del container[key]
                # make sure that attribute does not exist
                container.sync()
                with pytest.raises(AttributeError):
                    container[key].download("failed_download.zip")

            # override file content
            with open(filename, "wb") as file:
                file.write(content2)
            # then upload file2 to the same key
            container[key].upload_files([filename])

            # check if there's content of ONLY SECOND uploaded file
            container.sync()
            container[key].download("downloaded.zip")

            with ZipFile("downloaded.zip") as zipped:
                assert set(zipped.namelist()) == {filename, "/"}
                with zipped.open(filename, "r") as file:
                    content = file.read()
                    assert len(content) == len(content2)
                    assert content == content2
