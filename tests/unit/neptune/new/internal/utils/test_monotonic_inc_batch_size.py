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

import unittest

import pytest

from neptune.internal.utils.monotonic_inc_batch_size import MonotonicIncBatchSize


class TestMonotonicIncBatchSize(unittest.TestCase):
    def test_generating_increased_seq(self):
        batch_size = MonotonicIncBatchSize(size_limit=10, initial_size=1, scale_coef=1.5)

        assert batch_size.get() == 1
        batch_size.increase()
        assert batch_size.get() == 2
        batch_size.increase()
        assert batch_size.get() == 4
        batch_size.increase()
        assert batch_size.get() == 7
        assert batch_size.get() == 7
        batch_size.increase()
        assert batch_size.get() == 10
        assert batch_size.get() == 10
        batch_size.increase()
        assert batch_size.get() == 10

    def test_invalid_limit_size(self):
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=-1, initial_size=1, scale_coef=1.1)
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=0, initial_size=1, scale_coef=1.1)

    def test_invalid_scale_coef(self):
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=2, initial_size=1, scale_coef=1)
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=2, initial_size=1, scale_coef=-1)

    def test_invalid_initial_size(self):
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=2, initial_size=0, scale_coef=1.1)
        with pytest.raises(AssertionError):
            MonotonicIncBatchSize(size_limit=2, initial_size=3, scale_coef=1.1)
