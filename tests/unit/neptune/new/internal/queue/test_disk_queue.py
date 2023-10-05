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
import random
import threading
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory

import mock

from neptune.internal.queue.disk_queue import (
    DiskQueue,
    QueueElement,
)


def test_put():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
        ) as queue:
            # given
            obj = Obj(5, "test")
            queue.put(obj)

            # when
            queue.flush()

            # then
            assert get_queue_element(obj, 1) == queue.get()


def test_multiple_files():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=300,
        ) as queue:
            # given
            for i in range(1, 101):
                obj = Obj(i, str(i))
                queue.put(obj)

            # when
            queue.flush()

            # then
            for i in range(1, 101):
                obj = Obj(i, str(i))
                assert get_queue_element(obj, i) == queue.get()

            # and
            assert queue._read_file_version > 90
            assert queue._write_file_version > 90
            assert len(glob(dir_path + "/data-*.log")) > 10


def test_get_batch():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=100,
        ) as queue:
            # given
            for i in range(1, 91):
                obj = Obj(i, str(i))
                queue.put(obj)

            # when
            queue.flush()

            # then
            assert [get_queue_element(Obj(i, str(i)), i) for i in range(1, 26)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i) for i in range(26, 51)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i) for i in range(51, 76)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i) for i in range(76, 91)] == queue.get_batch(25)


def test_batch_limit():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=100,
            max_batch_size_bytes=get_obj_size_bytes(Obj(1, "1"), 1) * 3,
        ) as queue:
            # given
            for i in range(5):
                obj = Obj(i, str(i))
                queue.put(obj)

            # when
            queue.flush()

            # then
            assert [get_queue_element(Obj(i, str(i)), i + 1) for i in range(3)] == queue.get_batch(5)
            assert [get_queue_element(Obj(i, str(i)), i + 1) for i in range(3, 5)] == queue.get_batch(2)


def test_resuming_queue():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=999,
        ) as queue:
            # given
            for i in range(1, 501):
                obj = Obj(i, str(i))
                queue.put(obj)

            # when
            queue.flush()

            # and
            version = queue.get_batch(random.randrange(300, 400))[-1].ver
            version_to_ack = version - random.randrange(100, 200)
            queue.ack(version_to_ack)

            # then
            assert queue._read_file_version > 100
            assert queue._write_file_version > 450

            # and
            data_files = glob(dir_path + "/data-*.log")
            data_files_versions = [int(file[len(dir_path + "/data-") : -len(".log")]) for file in data_files]

            assert len(data_files) > 10
            assert 1 == len([ver for ver in data_files_versions if ver <= version_to_ack])

        # Resume queue
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=200,
        ) as queue:
            # then
            for i in range(version_to_ack + 1, 501):
                assert get_queue_element(Obj(i, str(i)), i) == queue.get()


def test_ack():
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=999,
        ) as queue:
            # given
            for i in range(5):
                queue.put(Obj(i, str(i)))

            # when
            queue.flush()

            # and
            queue.ack(3)

            # then
            assert get_queue_element(Obj(3, "3"), 4) == queue.get()
            assert get_queue_element(Obj(4, "4"), 5) == queue.get()


@mock.patch("shutil.rmtree")
def test_cleaning_up(rmtree):
    with TemporaryDirectory() as dir_path:
        with DiskQueue[Obj](
            dir_path=Path(dir_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=999,
        ) as queue:
            # given
            for i in range(5):
                queue.put(Obj(i, str(i)))

            # when
            queue.flush()

            # and
            queue.ack(5)

            # then
            assert 0 == queue.size()
            assert queue.is_empty()

            # when
            queue.cleanup_if_empty()

            # then
            assert rmtree.assert_called_once_with(Path(dir_path).resolve(), ignore_errors=True) is None


@dataclass
class Obj:
    num: int
    txt: str


def get_obj_size_bytes(obj, version) -> int:
    return len(json.dumps({"obj": obj.__dict__, "version": version}))


def get_queue_element(obj, version) -> QueueElement[Obj]:
    return QueueElement(obj, version, get_obj_size_bytes(obj, version))


def serializer(obj: "Obj") -> dict:
    return obj.__dict__


def deserializer(obj: dict) -> "Obj":
    return Obj(**obj)


def version_getter(obj: "Obj") -> int:
    return obj.num
