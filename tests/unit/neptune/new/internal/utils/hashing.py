#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from random import (
    choices,
    randint,
)
from string import (
    ascii_uppercase,
    digits,
)

from neptune.internal.utils.hashing import generate_hash


class TestHashGenerator:
    def test_should_be_deterministic(self):
        # given
        descriptors = [randint(0, 1024), "".join(choices(ascii_uppercase + digits, k=8))]

        # when
        hash1 = generate_hash(*descriptors, length=8)
        hash2 = generate_hash(*descriptors, length=8)

        # then
        assert hash1 == hash2

    def test_should_be_unique(self):
        # given
        unique_descriptors = set((randint(0, 1024), "".join(choices(ascii_uppercase + digits, k=8))) for _ in range(10))

        # when
        unique_hashes = set(generate_hash(*descriptors, length=8) for descriptors in unique_descriptors)

        # then
        assert len(unique_descriptors) == len(unique_hashes)
