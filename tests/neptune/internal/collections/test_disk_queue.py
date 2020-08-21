#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import json
import os
import unittest
from tempfile import TemporaryDirectory

from neptune.internal.collections.disk_queue import DiskQueue

# pylint: disable=protected-access


class TestDiskQueue(unittest.TestCase):

    class Obj:
        def __init__(self, num: int, txt: str):
            self.num = num
            self.txt = txt

        def __eq__(self, other):
            return isinstance(other, TestDiskQueue.Obj) and self.num == other.num and self.txt == other.txt

    def test_put(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](dirpath, "operations", self._serializer, self._deserializer)
            obj = TestDiskQueue.Obj(5, "test")
            queue.put(obj)
            queue.flush()
            self.assertEqual(queue.get(), obj)
            queue.close()

    def test_multiple_files(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](dirpath,
                                                 "operations",
                                                 self._serializer,
                                                 self._deserializer,
                                                 max_file_size=100)
            for i in range(0, 100):
                obj = TestDiskQueue.Obj(i, str(i))
                queue.put(obj)
            queue.flush()
            for i in range(0, 100):
                self.assertEqual(queue.get(), TestDiskQueue.Obj(i, str(i)))
            queue.close()
            self.assertTrue(queue._write_file_idx > 10)
            self.assertEqual(set(os.listdir(dirpath)),
                             set(["operations-{}.log".format(i) for i in range(0, queue._write_file_idx + 1)]))

    def test_get_batch(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](dirpath,
                                                 "operations",
                                                 self._serializer,
                                                 self._deserializer,
                                                 max_file_size=100)
            for i in range(0, 90):
                obj = TestDiskQueue.Obj(i, str(i))
                queue.put(obj)
            queue.flush()
            self.assertEqual(queue.get_batch(25), [TestDiskQueue.Obj(i, str(i)) for i in range(0, 25)])
            self.assertEqual(queue.get_batch(25), [TestDiskQueue.Obj(i, str(i)) for i in range(25, 50)])
            self.assertEqual(queue.get_batch(25), [TestDiskQueue.Obj(i, str(i)) for i in range(50, 75)])
            self.assertEqual(queue.get_batch(25), [TestDiskQueue.Obj(i, str(i)) for i in range(75, 90)])
            queue.close()

    @staticmethod
    def _serializer(obj: 'TestDiskQueue.Obj') -> str:
        return json.dumps(obj.__dict__)

    @staticmethod
    def _deserializer(obj: dict) -> 'TestDiskQueue.Obj':
        return TestDiskQueue.Obj(obj['num'], obj['txt'])
