import os
import uuid
from zipfile import ZipFile

import pytest

from e2e_tests.base import BaseE2ETest
from e2e_tests.standard.test_base import fake
from e2e_tests.utils import tmp_context
from neptune.new.attribute_container import AttributeContainer
from neptune.new.internal.backends.api_model import OptionalFeatures, MultipartConfig
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend


class TestUpload(BaseE2ETest):
    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_using_new_api(self, container: AttributeContainer):
        # pylint: disable=protected-access
        assert isinstance(container._backend, HostedNeptuneBackend)
        assert container._backend._client_config.has_feature(
            OptionalFeatures.MULTIPART_UPLOAD
        )
        assert isinstance(
            container._backend._client_config.multipart_config, MultipartConfig
        )

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    @pytest.mark.parametrize(
        "file_size",
        [
            pytest.param(10 * 2 ** 20, id="big"),  # 10 MB, multipart
            pytest.param(100 * 2 ** 10, id="small"),  # 100 kB, single upload
        ],
    )
    def test_single_file(self, container: AttributeContainer, file_size: int):
        key = self.gen_key()
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        with tmp_context():
            # create 10MB file
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

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_fileset(self, container: AttributeContainer):
        key = self.gen_key()
        large_filesize = 10 * 2 ** 20
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
