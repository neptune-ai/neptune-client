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
import time
from pathlib import Path

import pytest
from faker import Faker

from neptune.new.attribute_container import AttributeContainer

from tests.base import BaseE2ETest
from tests.utils import tmp_context, with_check_if_file_appears


fake = Faker()


class TestArtifacts(BaseE2ETest):
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_local_creation(self, container: AttributeContainer):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        with tmp_context() as tmp:
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            container[first].track_files('.')
            container[second].track_files(f'file://{tmp}')

            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_assignment(self, container: AttributeContainer):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        with tmp_context():
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            container[first].track_files(filename)
            container.wait()
            container[second] = container[first].fetch()
            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_local_download(self, container: AttributeContainer):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tmp_context() as tmp:
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            # Relative path
            container[first].track_files(filename)
            # Absolute path
            container[second].track_files(tmp)

            container.sync()

            with tmp_context():
                with with_check_if_file_appears(f'artifacts/{filename}'):
                    container[first].download('artifacts/')

                with with_check_if_file_appears(filepath):
                    container[second].download()

    @pytest.mark.s3
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_s3_creation(self, container: AttributeContainer, bucket):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, filename)

        container[first].track_files(f's3://{bucket_name}/{filename}')
        container[second].track_files(f's3://{bucket_name}/')

        container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.s3
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_s3_download(self, container: AttributeContainer, bucket):
        self.cleanup(container)

        first = self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, filename)
            s3_client.meta.client.upload_file(filepath, bucket_name, filepath)

        container[first].track_files(f's3://{bucket_name}/')

        container.sync()

        with tempfile.TemporaryDirectory() as tmp:
            with with_check_if_file_appears(f'{tmp}/{filename}'):
                container[first].download(tmp)

        with tmp_context():
            with with_check_if_file_appears(filename):
                container[first].download()

    @pytest.mark.s3
    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_s3_existing(self, container: AttributeContainer, bucket):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3_client = bucket

        with tmp_context():
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3_client.meta.client.upload_file(filename, bucket_name, filename)
            s3_client.meta.client.upload_file(filepath, bucket_name, filepath)

        # Track all files - "a" and "b" to first artifact
        container[first].track_files(f's3://{bucket_name}/')

        # Track only the "a" file to second artifact
        container[second].track_files(f's3://{bucket_name}/{filename}')
        container.sync()

        # Add "b" file to existing second artifact
        # so it should be now identical as first
        container[second].track_files(f's3://{bucket_name}/{filepath}', destination=str(Path(filepath).parent))
        container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_local_existing(self, container: AttributeContainer):
        self.cleanup(container)

        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tmp_context() as tmp:
            with open(filename, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            # Track all files - "a" and "b" to first artifact
            container[first].track_files('.')

            # Track only the "a" file to second artifact
            container[second].track_files(f'file://{tmp}/{filename}')
            container.sync()

            # Add "b" file to existing second artifact
            # so it should be now identical as first
            container[second].track_files(filepath, destination=str(Path(filepath).parent))
            container.sync()

        assert container[first].fetch_hash() == container[second].fetch_hash()
        assert container[first].fetch_files_list() == container[second].fetch_files_list()

    @pytest.mark.parametrize('container', ['project', 'run'], indirect=True)
    def test_hash_cache(self, container: AttributeContainer):
        self.cleanup(container)

        key = self.gen_key()
        filename = fake.file_name()

        with tmp_context():
            # create 2GB file
            with open(filename, 'wb') as handler:
                handler.write(b'\0' * 2 * 2 ** 30)

            # track it
            start = time.time()
            container[key].track_files('.', wait=True)
            initial_duration = time.time() - start

            # and track it again
            start = time.time()
            container[key].track_files('.', wait=True)
            retry_duration = time.time() - start

            assert retry_duration * 2 < initial_duration, "Tracking again should be significantly faster"

            # append additional byte to file
            with open(filename, 'ab') as handler:
                handler.write(b'\0')

            # and track updated file
            start = time.time()
            container[key].track_files('.', wait=True)
            updated_duration = time.time() - start

            assert retry_duration * 2 < updated_duration, "Tracking updated file should take more time - no cache"
