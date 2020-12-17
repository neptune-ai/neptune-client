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
import math
import random
import unittest
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable

from neptune.alpha.internal.containers.disk_queue import DiskQueue

# pylint: disable=protected-access


class TestDiskQueue(unittest.TestCase):

    class Obj:
        def __init__(self, num: int, txt: str, use_blob_storage: bool = False):
            self.num = num
            self.txt = txt
            self.use_blob_storage = use_blob_storage

        def __eq__(self, other):
            return isinstance(other, TestDiskQueue.Obj) and self.num == other.num and self.txt == other.txt

    def test_put(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](Path(dirpath), self._serializer, self._deserializer)
            obj = TestDiskQueue.Obj(5, "test")
            queue.put(obj)
            queue.flush()
            self.assertEqual(queue.get(), (obj, 1))
            queue.close()

    def test_multiple_files(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](Path(dirpath), self._serializer, self._deserializer, max_file_size=300)
            for i in range(1, 101):
                obj = TestDiskQueue.Obj(i, str(i))
                queue.put(obj)
            queue.flush()
            for i in range(1, 101):
                self.assertEqual(queue.get(), (TestDiskQueue.Obj(i, str(i)), i))
            queue.close()
            self.assertTrue(queue._read_file_version > 90)
            self.assertTrue(queue._write_file_version > 90)
            self.assertTrue(len(glob(dirpath + "/data-*.log")) > 10)

    def test_get_batch(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](Path(dirpath), self._serializer, self._deserializer, max_file_size=100)
            for i in range(1, 91):
                obj = TestDiskQueue.Obj(i, str(i))
                queue.put(obj)
            queue.flush()
            self.assertEqual(queue.get_batch(25), ([TestDiskQueue.Obj(i, str(i)) for i in range(1, 26)], 25))
            self.assertEqual(queue.get_batch(25), ([TestDiskQueue.Obj(i, str(i)) for i in range(26, 51)], 50))
            self.assertEqual(queue.get_batch(25), ([TestDiskQueue.Obj(i, str(i)) for i in range(51, 76)], 75))
            self.assertEqual(queue.get_batch(25), ([TestDiskQueue.Obj(i, str(i)) for i in range(76, 91)], 90))
            queue.close()

    def test_resuming_queue(self):
        with TemporaryDirectory() as dirpath:
            queue = DiskQueue[TestDiskQueue.Obj](Path(dirpath), self._serializer, self._deserializer, max_file_size=999)
            for i in range(1, 501):
                obj = TestDiskQueue.Obj(i, str(i), (i % 50) == 0)
                queue.put(obj)
            queue.flush()
            _, version = queue.get_batch(random.randrange(300, 400))
            version_to_ack = version - random.randrange(100, 200)
            queue.ack(version_to_ack)

            self.assertTrue(queue._read_file_version > 100)
            self.assertTrue(queue._write_file_version > 450)

            log_files = glob(dirpath + "/data-*.log")
            self.assertTrue(len(log_files) > 10)
            log_file_versions = [int(file[len(dirpath + "/data-"):-len(".log")]) for file in log_files]
            self.assertTrue(len([ver for ver in log_file_versions if ver <= version_to_ack]) == 1)

            blob_files = glob(dirpath + "/data-*.blob")
            self.assertEqual(len(blob_files), math.ceil((500 - version_to_ack) / 50))
            blob_file_versions = [int(file[len(dirpath + "/data-"):-len(".blob")]) for file in blob_files]
            self.assertTrue(len([ver for ver in blob_file_versions if ver <= version_to_ack]) == 0)

            queue.close()

            queue = DiskQueue[TestDiskQueue.Obj](Path(dirpath), self._serializer, self._deserializer, max_file_size=200)
            for i in range(version_to_ack + 1, 501):
                self.assertEqual(queue.get(), (TestDiskQueue.Obj(i, str(i)), i))

            queue.close()

    @staticmethod
    def _serializer(obj: 'TestDiskQueue.Obj', blob_storage_supplier: Callable[[], str]) -> dict:
        if obj.use_blob_storage:
            with open(blob_storage_supplier(), "w") as file:
                file.write("test content")
        return obj.__dict__

    @staticmethod
    def _deserializer(obj: dict) -> 'TestDiskQueue.Obj':
        return TestDiskQueue.Obj(obj['num'], obj['txt'])

    @staticmethod
    def _version_getter(obj: 'TestDiskQueue.Obj') -> int:
        return obj.num
