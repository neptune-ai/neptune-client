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
import random

from faker import Faker

from tests.e2e.base import BaseE2ETest

fake = Faker()


class TestSeries(BaseE2ETest):
    def test_log_numbers(self, run):
        key = self.gen_key()
        values = [random.random() for _ in range(50)]

        run[key].log(values[0])
        run[key].log(values[1:])
        run.sync()

        assert run[key].fetch_last() == values[-1]

        fetched_values = run[key].fetch_values()
        assert list(fetched_values['value']) == values

    def test_log_strings(self, run):
        key = self.gen_key()
        values = [fake.word() for _ in range(50)]

        run[key].log(values[0])
        run[key].log(values[1:])
        run.sync()

        assert run[key].fetch_last() == values[-1]

        fetched_values = run[key].fetch_values()
        assert list(fetched_values['value']) == values
