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
from contextlib import contextmanager
from datetime import datetime
import abc


class ClientFeatures(abc.ABC):
    params = {
        'init_text_parameter': 'some text',
        'init_number parameter': 42,
        'init_list': [1, 2, 3],
        'init_datetime': datetime.now()
    }
    properties = {
        'init_text_property': 'some text',
        'init_number property': 42,
        'init_list': [1, 2, 3],
        'init_datetime': datetime.now(),
    }

    data_dir = 'alpha_integration_dev/data'
    img_path = f'{data_dir}/g.png'
    text_file_path = f'{data_dir}/text.txt'

    @contextmanager
    def with_assert_raises(self, exception):
        """Checks given exception is raised inside block,
        raises AssertionError otherwise"""
        try:
            yield
        except exception:
            pass
        else:
            raise AssertionError(f'Should raise {exception}')

    @staticmethod
    def _remove_file_if_exists(filepath):
        try:
            os.remove(filepath)
        except OSError:
            pass

    @contextmanager
    def with_check_if_file_appears(self, filepath):
        """Checks if file will be present when leaving the block.
        File is removed if exists when entering the block."""
        self._remove_file_if_exists(filepath)

        try:
            yield
            assert os.path.exists(filepath)
        except:
            raise
        else:
            self._remove_file_if_exists(filepath)

    @abc.abstractmethod
    def modify_tags(self):
        pass

    @abc.abstractmethod
    def modify_properties(self):
        pass

    @abc.abstractmethod
    def log_std(self):
        """system streams / monitoring logs"""

    @abc.abstractmethod
    def log_series(self):
        pass

    @abc.abstractmethod
    def handle_files_and_images(self):
        pass

    @abc.abstractmethod
    def handle_directories(self):
        pass

    @abc.abstractmethod
    def finalize(self):
        pass

    def run(self):
        self.modify_tags()
        self.modify_properties()
        self.log_std()
        self.log_series()
        self.handle_files_and_images()
        self.handle_directories()

        self.finalize()
