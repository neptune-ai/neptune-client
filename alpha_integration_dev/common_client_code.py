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

from datetime import datetime
import abc


class ClientFeatures(abc.ABC):
    params = {
        'text_parameter': 'some text',
        'number parameter': 42,
        'list': [1, 2, 3],
        'datetime': datetime.now()
    }

    img_path = 'alpha_integration_dev/data/g.png'

    @abc.abstractmethod
    def modify_tags(self):
        """NPT-9213"""

    @abc.abstractmethod
    def log_std(self):
        """system streams / monitoring logs"""

    @abc.abstractmethod
    def log_series(self):
        pass

    @abc.abstractmethod
    def handle_files_and_images(self):
        """NPT-9207"""

    @abc.abstractmethod
    def other(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass
