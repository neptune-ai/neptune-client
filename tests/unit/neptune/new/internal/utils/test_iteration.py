#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from neptune.internal.utils.iteration import get_batches


class TestIterationUtils(unittest.TestCase):
    def test_get_batches(self):
        self.assertEqual([[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]], list(get_batches(range(10), batch_size=3)))
        self.assertEqual([[0, 1, 2], [3, 4, 5], [6, 7, 8]], list(get_batches(range(9), batch_size=3)))
        self.assertEqual([[1], [2], [3]], list(get_batches([1, 2, 3], batch_size=1)))
        self.assertEqual([[1], [2], [3]], list(get_batches(iter([1, 2, 3]), batch_size=1)))
        self.assertEqual([[1, 2, 3]], list(get_batches([1, 2, 3], batch_size=100)))

        with self.assertRaises(AssertionError):
            next(get_batches([1, 2, 3], batch_size=0))

        # but generator itself doesn't raise error untill used
        get_batches([1, 2, 3], batch_size=0)

    def test_batch_lists(self):
        list_of_lists = (list(range(i)) for i in range(10))
        expected_batched = [
            [[], [0], [0, 1]],
            [[0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]],
            [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5, 6], [0, 1, 2, 3, 4, 5, 6, 7]],
            [[0, 1, 2, 3, 4, 5, 6, 7, 8]],
        ]
        self.assertEqual(expected_batched, list(get_batches(list_of_lists, batch_size=3)))
