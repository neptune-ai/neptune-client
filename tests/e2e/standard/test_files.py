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
import io
import os
import random
import uuid
from itertools import product
from typing import Set
from unittest.mock import Mock
from zipfile import ZipFile

import pytest

from neptune.internal.backends import hosted_file_operations
from neptune.internal.backends.api_model import (
    MultipartConfig,
    OptionalFeatures,
)
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.types.file_types import FileType
from neptune.metadata_containers import MetadataContainer
from neptune.types import (
    File,
    FileSet,
)
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)
from tests.e2e.plot_utils import (
    generate_altair_chart,
    generate_brokeh_figure,
    generate_matplotlib_figure,
    generate_pil_image,
    generate_plotly_figure,
    generate_seaborn_figure,
)
from tests.e2e.utils import (
    SIZE_1KB,
    SIZE_1MB,
    initialize_container,
    preserve_cwd,
    tmp_context,
)


class TestUpload(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_using_new_api(self, container: MetadataContainer):
        assert isinstance(container._backend, HostedNeptuneBackend)
        assert container._backend._client_config.has_feature(OptionalFeatures.MULTIPART_UPLOAD)
        assert isinstance(container._backend._client_config.multipart_config, MultipartConfig)

    def _test_upload(self, container: MetadataContainer, file_type: FileType, file_size: int):
        key = self.gen_key()
        extension = fake.file_extension()
        downloaded_filename = fake.file_name()
        content = os.urandom(file_size)

        if file_type is FileType.LOCAL_FILE:
            filename = fake.file_name(extension=extension)
            with open(filename, "wb") as file:
                file.write(content)

            file = File.from_path(filename)
        elif file_type is FileType.IN_MEMORY:
            file = File.from_content(content, extension=extension)
        elif file_type is FileType.STREAM:
            file = File.from_stream(io.BytesIO(content), extension=extension)
        else:
            raise ValueError()

        container[key].upload(file)
        container.sync()
        container[key].download(downloaded_filename)

        assert container[key].fetch_extension() == extension
        assert os.path.getsize(downloaded_filename) == file_size
        with open(downloaded_filename, "rb") as file:
            downloaded_content = file.read()
            assert len(content) == file_size
            assert downloaded_content == content

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    @pytest.mark.parametrize("file_type", list(FileType))
    def test_single_upload(self, container: MetadataContainer, file_type: FileType):
        file_size = 100 * SIZE_1KB  # 100 kB, single upload
        self._test_upload(container, file_type, file_size)

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_multipart_upload(self, container: MetadataContainer):
        file_size = 10 * SIZE_1MB  # 10 MB, multipart
        self._test_upload(container, FileType.IN_MEMORY, file_size)

    def test_file_changed_during_upload(self, environment, monkeypatch):
        key = self.gen_key()
        file_size = 11 * SIZE_1MB  # 11 MB, multipart with 3 parts
        intermediate_size = 6 * SIZE_1MB  # 6 MB, second part < 5MB
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        _upload_raw_data = hosted_file_operations.upload_raw_data

        run = initialize_container(
            container_type="run",
            project=environment.project,
            mode="sync",
        )

        with tmp_context():
            # create file_size file
            with open(filename, "wb") as file:
                file.write(b"\0" * file_size)

            class UploadedFileChanger:
                def __init__(self):
                    self.upload_part_iteration = 0

                def __call__(self, *args, **kwargs):
                    # file starts to change and after uploading first part it's at intermediate_size
                    if self.upload_part_iteration == 0:
                        with open(filename, "wb") as file:
                            file.write(b"\0" * intermediate_size)
                    # after that it's back at file_size
                    elif self.upload_part_iteration == 1:
                        with open(filename, "wb") as file:
                            file.write(b"\0" * file_size)
                    self.upload_part_iteration += 1

                    return _upload_raw_data(*args, **kwargs)

            hacked_upload_raw_data = UploadedFileChanger()
            monkeypatch.setattr(
                hosted_file_operations,
                "upload_raw_data",
                Mock(wraps=hacked_upload_raw_data),
            )

            run[key].upload(filename)

            run[key].download(downloaded_filename)

            assert os.path.getsize(downloaded_filename) == file_size
            with open(downloaded_filename, "rb") as file:
                content = file.read()
                assert len(content) == file_size
                assert content == b"\0" * file_size
            # handling restart + 3 for actual upload
            assert hacked_upload_raw_data.upload_part_iteration == 5

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_replace_float_attribute_with_uploaded_file(self, container: MetadataContainer):
        key = self.gen_key()
        file_size = 100 * SIZE_1KB  # 100 kB
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        with tmp_context():
            # create file_size file
            with open(filename, "wb") as file:
                file.write(b"\0" * file_size)

            # set key to a float and sync it separately
            container[key] = 42.0
            container.sync()

            # delete and upload in the same queue flush
            container[key].pop()
            container[key].upload(filename)

            container.sync()
            container[key].download(downloaded_filename)

            assert os.path.getsize(downloaded_filename) == file_size
            with open(downloaded_filename, "rb") as file:
                content = file.read()
                assert len(content) == file_size
                assert content == b"\0" * file_size

    def test_upload_with_changed_working_directory(self, environment):
        os.makedirs("some_other_folder", exist_ok=True)

        key_in_mem = self.gen_key()
        key_from_disk = self.gen_key()

        with preserve_cwd("some_other_folder"):
            run = initialize_container(container_type="run", project=environment.project)
            # upload file from memory
            run[key_in_mem].upload(File.from_content("abcd"))

            # upload file from disk
            filename = fake.file_name()
            with open(filename, "w") as fp:
                fp.write("test content")

            run[key_from_disk].upload(filename)

            run.sync()

        assert run.exists(key_in_mem)
        assert run.exists(key_from_disk)
        run.stop()


class TestFileSet(BaseE2ETest):
    def _test_fileset(self, container: MetadataContainer, large_file_size: int, small_files_no: int):
        key = self.gen_key()
        large_filename = fake.file_name()
        small_files = [
            (f"{uuid.uuid4()}.{fake.file_extension()}", fake.sentence().encode("utf-8")) for _ in range(small_files_no)
        ]

        with tmp_context():
            # create single large file (multipart) and a lot of very small files
            with open(large_filename, "wb") as file:
                file.write(b"\0" * large_file_size)
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
                    assert len(content) == large_file_size
                    assert content == b"\0" * large_file_size

            # when small files as fileset uploaded
            container[key].upload_files(small_filenames)

            # then check if everything will be downloaded
            container.sync()
            container[key].download("downloaded2.zip")

            with ZipFile("downloaded2.zip") as zipped:
                assert set(zipped.namelist()) == {large_filename, "/", *small_filenames}
                with zipped.open(large_filename, "r") as file:
                    content = file.read()
                    assert len(content) == large_file_size
                    assert content == b"\0" * large_file_size
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

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_fileset(self, container: MetadataContainer):
        # 100 kB, single upload for large file
        large_file_size = 100 * SIZE_1KB
        small_files_no = 10
        self._test_fileset(container, large_file_size, small_files_no)

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_fileset_with_multipart(self, container: MetadataContainer):
        # 10 MB, multipart upload for large file
        large_file_size = 10 * SIZE_1MB
        small_files_no = 100
        self._test_fileset(container, large_file_size, small_files_no)

    @classmethod
    def _gen_tree_paths(cls, depth, width=2) -> Set:
        """Generates all subdirectories of some random tree directory structure"""
        this_level_dirs = (fake.word() + "/" for _ in range(width))
        if depth == 1:
            return set(this_level_dirs)
        else:
            subpaths = cls._gen_tree_paths(depth=depth - 1, width=width)
            new_paths = set("".join(prod) for prod in product(subpaths, this_level_dirs))
            subpaths.update(new_paths)
            return subpaths

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_fileset_nested_structure(self, container: MetadataContainer):
        key = self.gen_key()
        possible_paths = self._gen_tree_paths(depth=3)

        small_files = [
            (
                f"{path}{uuid.uuid4()}.{fake.file_extension()}",
                os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB)),
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

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_reset_fileset(self, container: MetadataContainer):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()
        content1 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))
        content2 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))

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

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    @pytest.mark.parametrize("delete_attribute", [True, False])
    def test_single_file_override(self, container: MetadataContainer, delete_attribute: bool):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()
        content1 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))
        content2 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))
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

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    @pytest.mark.parametrize("delete_attribute", [True, False])
    def test_fileset_file_override(self, container: MetadataContainer, delete_attribute: bool):
        key = self.gen_key()
        filename = fake.file_name()
        content1 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))
        content2 = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))

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

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_list_fileset_files(self, container: MetadataContainer):
        key = self.gen_key()
        filename = fake.file_name()
        content = os.urandom(random.randint(SIZE_1KB, 100 * SIZE_1KB))

        with tmp_context():
            # create file
            with open(filename, "wb") as file1:
                file1.write(content)

            file_size = os.path.getsize(filename)
            # upload file1 to key
            container[key].upload_files([filename])
            container.sync()

            file_list = container[key].list_fileset_files()
            assert len(file_list) == 1
            assert file_list[0].name == filename
            assert file_list[0].file_type == "file"
            assert file_list[0].size == file_size

            container[key].delete_files(filename)
            container.sync()
            assert container[key].list_fileset_files() == []


class TestPlotObjectsAssignment(BaseE2ETest):
    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_pil_image(self, container: MetadataContainer):
        pil_image = generate_pil_image()
        container["pil_image"] = pil_image

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_matplotlib_figure(self, container: MetadataContainer):
        figure = generate_matplotlib_figure()
        container["matplotlib_figure"] = figure

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_altair_chart(self, container: MetadataContainer):
        altair_chart = generate_altair_chart()
        container["altair_chart"] = altair_chart

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_brokeh_figure(self, container: MetadataContainer):
        brokeh_figure = generate_brokeh_figure()
        container["brokeh_figure"] = brokeh_figure

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_plotly_figure(self, container: MetadataContainer):
        plotly_figure = generate_plotly_figure()
        container["plotly_figure"] = plotly_figure

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_seaborn_figure(self, container: MetadataContainer):
        seaborn_figure = generate_seaborn_figure()
        container["seaborn_figure"] = seaborn_figure
