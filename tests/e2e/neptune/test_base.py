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
from datetime import datetime, timezone

import pytest
from faker import Faker

from neptune.new.exceptions import MissingFieldException
from tests.e2e.base import BaseE2ETest

fake = Faker()


class TestAtoms(BaseE2ETest):
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_simple_assign_and_fetch(self, run, value):
        key = self.gen_key()

        run[key] = value
        run.sync()
        assert run[key].fetch() == value

    def test_simple_assign_datetime(self, run):
        key = self.gen_key()
        now = datetime.now()

        run[key] = now
        run.sync()

        # expect truncate to milliseconds and add UTC timezone
        expected_now = now.astimezone(timezone.utc).replace(microsecond=int(now.microsecond / 1000) * 1000)
        assert run[key].fetch() == expected_now

    def test_fetch_non_existing_key(self, run):
        key = self.gen_key()
        with pytest.raises(MissingFieldException):
            run[key].fetch()

    def test_delete_atom(self, run):
        key = self.gen_key()
        value = fake.name()

        run[key] = value
        run.sync()

        assert run[key].fetch() == value

        del run[key]
        with pytest.raises(MissingFieldException):
            run[key].fetch()


class TestStringSet:
    neptune_tags_path = 'sys/tags'

    def test_do_not_accept_non_tag_path(self, run):
        random_path = 'some/path'
        run[random_path].add(fake.unique.word())
        run.sync()

        with pytest.raises(MissingFieldException):
            # backends accepts `'sys/tags'` only
            run[random_path].fetch()

    def test_add_and_remove_tags(self, run):
        remaining_tag1 = fake.unique.word()
        remaining_tag2 = fake.unique.word()
        to_remove_tag1 = fake.unique.word()
        to_remove_tag2 = fake.unique.word()

        run[self.neptune_tags_path].add(remaining_tag1)
        run[self.neptune_tags_path].add([to_remove_tag1, remaining_tag2])
        run[self.neptune_tags_path].remove(to_remove_tag1)
        run[self.neptune_tags_path].remove(to_remove_tag2)  # remove non existing tag
        run.sync()

        assert run[self.neptune_tags_path].fetch() == {remaining_tag1, remaining_tag2}
