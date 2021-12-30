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
import random
import time
import uuid
from datetime import datetime, timezone
from zipfile import ZipFile

import pytest
from faker import Faker

import neptune.new as neptune
from neptune.new.attribute_container import AttributeContainer

from tests.base import BaseE2ETest
from tests.utils import tmp_context

fake = Faker()


class TestAtoms(BaseE2ETest):
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_simple_assign_and_fetch(self, container: AttributeContainer, value):
        key = self.gen_key()

        container[key] = value
        container.sync()
        assert container[key].fetch() == value

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_simple_assign_datetime(self, container: AttributeContainer):
        key = self.gen_key()
        now = datetime.now()

        container[key] = now
        container.sync()

        # expect truncate to milliseconds and add UTC timezone
        expected_now = now.astimezone(timezone.utc).replace(microsecond=int(now.microsecond / 1000) * 1000)
        assert container[key].fetch() == expected_now

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_fetch_non_existing_key(self, container: AttributeContainer):
        key = self.gen_key()
        with pytest.raises(AttributeError):
            container[key].fetch()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_delete_atom(self, container: AttributeContainer):
        key = self.gen_key()
        value = fake.name()

        container[key] = value
        container.sync()

        assert container[key].fetch() == value

        del container[key]
        with pytest.raises(AttributeError):
            container[key].fetch()


class TestNamespace(BaseE2ETest):
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_reassigning(self, container: AttributeContainer):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = fake.name()

        # Assign a namespace
        container[namespace] = {
            f"{key}": value
        }
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        # Direct reassign internal value
        value = fake.name()
        container[f"{namespace}/{key}"] = value
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        # Reassigning by namespace
        value = fake.name()
        container[namespace] = {
            f"{key}": value
        }
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_distinct_types(self, container: AttributeContainer):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = random.randint(0, 100)

        container[namespace] = {
            f"{key}": value
        }
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        new_value = fake.name()

        with pytest.raises(ValueError):
            container[namespace] = {
                f"{key}": new_value
            }
            container.sync()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_delete_namespace(self, container: AttributeContainer):
        namespace = fake.unique.word()
        key1 = fake.unique.word()
        key2 = fake.unique.word()
        value1 = fake.name()
        value2 = fake.name()

        container[namespace][key1] = value1
        container[namespace][key2] = value2
        container.sync()

        assert container[namespace][key1].fetch() == value1
        assert container[namespace][key2].fetch() == value2

        del container[namespace]
        with pytest.raises(AttributeError):
            container[namespace][key1].fetch()
        with pytest.raises(AttributeError):
            container[namespace][key2].fetch()


class TestStringSet:
    neptune_tags_path = 'sys/tags'

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_do_not_accept_non_tag_path(self, container: AttributeContainer):
        random_path = 'some/path'
        container[random_path].add(fake.unique.word())
        container.sync()

        with pytest.raises(AttributeError):
            # backends accepts `'sys/tags'` only
            container[random_path].fetch()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_add_and_remove_tags(self, container: AttributeContainer):
        remaining_tag1 = fake.unique.word()
        remaining_tag2 = fake.unique.word()
        to_remove_tag1 = fake.unique.word()
        to_remove_tag2 = fake.unique.word()

        container.sync()
        if container.exists(self.neptune_tags_path):
            container[self.neptune_tags_path].clear()
        container[self.neptune_tags_path].add(remaining_tag1)
        container[self.neptune_tags_path].add([to_remove_tag1, remaining_tag2])
        container[self.neptune_tags_path].remove(to_remove_tag1)
        container[self.neptune_tags_path].remove(to_remove_tag2)  # remove non existing tag
        container.sync()

        assert container[self.neptune_tags_path].fetch() == {remaining_tag1, remaining_tag2}


class TestFiles(BaseE2ETest):
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_file(self, container: AttributeContainer):
        key = self.gen_key()
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        with tmp_context():
            # create 10MB file
            with open(filename, "wb") as file:
                file.write(b"\0" * 10 * 2 ** 20)
            container[key].upload(filename)

            container.sync()
            container[key].download(downloaded_filename)

            assert os.path.getsize(downloaded_filename) == 10 * 2 ** 20
            with open(downloaded_filename, "rb") as file:
                content = file.read()
                assert len(content) == 10 * 2 ** 20
                assert content == b"\0" * 10 * 2 ** 20

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_fileset(self, container: AttributeContainer):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()

        with tmp_context():
            # create two 10MB files
            with open(filename1, "wb") as file1, open(filename2, "wb") as file2:
                file1.write(b"\0" * 10 * 2 ** 20)
                file2.write(b"\0" * 10 * 2 ** 20)

            # when one file as fileset uploaded
            container[key].upload_files([filename1])

            # then check if will be downloaded
            container.sync()
            container[key].download("downloaded1.zip")

            with ZipFile("downloaded1.zip") as zipped:
                assert set(zipped.namelist()) == {filename1, "/"}
                with zipped.open(filename1, "r") as file1:
                    content1 = file1.read()
                    assert len(content1) == 10 * 2 ** 20
                    assert content1 == b"\0" * 10 * 2 ** 20

            # when second file as fileset uploaded
            container[key].upload_files([filename2])

            # then check if both will be downloaded
            container.sync()
            container[key].download("downloaded2.zip")

            with ZipFile("downloaded2.zip") as zipped:
                assert set(zipped.namelist()) == {filename1, filename2, "/"}
                with zipped.open(filename1, "r") as file1,\
                        zipped.open(filename2, "r") as file2:
                    content1 = file1.read()
                    content2 = file2.read()
                    assert len(content1) == len(content2) == 10 * 2 ** 20
                    assert content1 == content2 == b"\0" * 10 * 2 ** 20

            # when first file is removed
            container[key].delete_files([filename1])

            # then check if second will be downloaded
            container.sync()
            container[key].download("downloaded3.zip")

            with ZipFile("downloaded3.zip") as zipped:
                assert set(zipped.namelist()) == {filename2, "/"}
                with zipped.open(filename2, "r") as file2:
                    content2 = file2.read()
                    assert len(content2) == 10 * 2 ** 20
                    assert content2 == b"\0" * 10 * 2 ** 20


class TestFetchRunsTable(BaseE2ETest):
    def test_fetch_table(self):
        tag = str(uuid.uuid4())
        with neptune.init() as run:
            run["sys/tags"].add(tag)
            run["value"] = 12

        with neptune.init() as run:
            run["sys/tags"].add(tag)
            run["another/value"] = "testing"

        # wait for the elasticsearch cache to fill
        time.sleep(1)

        project = neptune.init_project()

        runs_table = sorted(project.fetch_runs_table(tag=tag).to_runs(), key=lambda r: r.get_attribute_value("sys/id"))
        assert len(runs_table) == 2
        assert runs_table[0].get_attribute_value("value") == 12
        assert runs_table[1].get_attribute_value("another/value") == "testing"
