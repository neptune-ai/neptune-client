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

import neptune.new as neptune
from tests.e2e.base import BaseE2ETest
from tests.e2e.utils import with_check_if_file_appears
from faker import Faker

fake = Faker()


class TestInit(BaseE2ETest):
    # TODO: test all remaining init parameters
    def test_resuming_exp(self):
        key = self.gen_key()

        exp = neptune.init()
        val = fake.word()
        exp[key] = val
        exp.stop()

        exp.sync()

        exp2 = neptune.init(run=exp._short_id)
        assert exp2[key].fetch() == val

    def test_send_source_code(self):
        exp = neptune.init(
            source_files='**/*.py'
        )

        # download sources
        exp.sync()
        with with_check_if_file_appears('files.zip'):
            exp['source_code/files'].download()
