#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
from typing import (
    List,
    Optional,
    Tuple,
)

import pytest
from mock import patch
from pytest import fixture

from neptune.core.components.queue.aggregating_disk_queue import (
    AggregatingDiskQueue,
    CategoryQueueElement,
)
from neptune.core.components.queue.disk_queue import QueueElement


@fixture(autouse=True, scope="function")
def mock_time():
    with patch("neptune.core.components.queue.disk_queue.time") as time_mock:
        time_mock.side_effect = list(range(1234, 1234 + 1000))
        yield time_mock


def test_put():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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
            assert get_queue_element(obj, 1, 1234) == queue.get()


def test_multiple_files():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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
                assert get_queue_element(obj, i, 1234 + i - 1) == queue.get()

            # and
            assert queue._disk_queue._read_file_version > 90
            assert queue._disk_queue._write_file_version > 90
            assert len(glob(data_path + "/data-*.log")) > 10


def test_get_batch_no_category():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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
            assert [get_queue_element(Obj(i, str(i)), i, 1234 + i - 1) for i in range(1, 26)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i, 1234 + i - 1) for i in range(26, 51)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i, 1234 + i - 1) for i in range(51, 76)] == queue.get_batch(25)
            assert [get_queue_element(Obj(i, str(i)), i, 1234 + i - 1) for i in range(76, 91)] == queue.get_batch(25)


@pytest.mark.parametrize(
    "category_series_and_expected",
    [
        ([1, 1, 1, 1, 1], [(5, 1)]),
        ([1, 1, None, None, 1, 1], [(6, 1)]),
        ([1, 1, 1, 2, 2, 2], [(3, 1), (3, 2)]),
        ([1, 1, 1, None, None, 2, 2, 2], [(5, 1), (3, 2)]),
        ([None, None, None, 1], [(4, 1)]),
        ([None, None, None, None], [(4, None)]),
    ],
)
def test_get_batch_with_category(category_series_and_expected: Tuple[List[Optional[int]], List[Tuple[int, int]]]):
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=100,
        ) as queue:
            category_series, expected_batch_summary = category_series_and_expected
            # given
            for i, cat in enumerate(category_series):
                obj = Obj(i, str(i))
                queue.put(obj, category=cat)

            # when
            queue.flush()

            for expected_batch_size, expected_category in expected_batch_summary:
                batch = queue.get_batch(len(category_series) + 1)
                assert expected_batch_size == len(batch)
                assert all(
                    [expected_category == element.obj.category for element in batch if element.obj.category is not None]
                )


def test_batch_limit():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=100,
            max_batch_size_bytes=get_obj_size_bytes(Obj(1, "1"), 1, 1234) * 3,
        ) as queue:
            # given
            for i in range(5):
                obj = Obj(i, str(i))
                queue.put(obj)

            # when
            queue.flush()

            # then
            assert [get_queue_element(Obj(i, str(i)), i + 1, 1234 + i) for i in range(3)] == queue.get_batch(5)
            assert [get_queue_element(Obj(i, str(i)), i + 1, 1234 + i) for i in range(3, 5)] == queue.get_batch(2)


def test_resuming_queue_when_on_new_category():
    category_series = [1, 1, 1, 2, 2, 2]
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=999,
        ) as queue:
            # given
            for i, category in enumerate(category_series):
                obj = Obj(i, str(i))
                queue.put(obj, category=category)

            # when
            queue.flush()

            # and
            batch_1 = queue.get_batch(10)

            assert [get_queue_element(Obj(i, str(i)), i + 1, 1234 + i, category=1) for i in range(3)] == batch_1
            assert len(batch_1) == 3
            assert all([e.obj.category == 1 for e in batch_1])
            version = batch_1[-1].ver
            queue.ack(version)

        # Resume queue
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=200,
        ) as queue:
            # then
            batch_2 = queue.get_batch(10)
            assert len(batch_2) == 3
            assert all([e.obj.category == 2 for e in batch_2])
            assert [get_queue_element(Obj(i, str(i)), i + 1, 1234 + i, category=2) for i in range(3, 6)] == batch_2


def test_resuming_queue():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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

            # and
            data_files = glob(data_path + "/data-*.log")
            data_files_versions = [int(file[len(data_path + "/data-") : -len(".log")]) for file in data_files]

            assert len(data_files) > 10
            assert 1 == len([ver for ver in data_files_versions if ver <= version_to_ack])

        # Resume queue
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
            to_dict=serializer,
            from_dict=deserializer,
            lock=threading.RLock(),
            max_file_size=200,
        ) as queue:
            # then
            for i in range(version_to_ack + 1, 501):
                assert get_queue_element(Obj(i, str(i)), i, 1234 + i - 1) == queue.get()


def test_ack():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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
            assert get_queue_element(Obj(3, "3"), 4, 1234 + 3) == queue.get()
            assert get_queue_element(Obj(4, "4"), 5, 1234 + 4) == queue.get()


def test_cleaning_up():
    with TemporaryDirectory() as data_path:
        with AggregatingDiskQueue[Obj, int](
            data_path=Path(data_path),
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

    assert list(Path(data_path).glob("*")) == []


@dataclass
class Obj:
    num: int
    txt: str


def get_obj_size_bytes(obj: Obj, version, at: Optional[int] = None, category: Optional[int] = None) -> int:
    cat_object = {"obj": obj.__dict__, "cat": category}
    queue_element = {"obj": cat_object, "version": version, "at": at}
    return len(json.dumps(queue_element))


def get_queue_element(
    obj: Obj, version, at: Optional[int] = None, category: Optional[int] = None
) -> QueueElement[CategoryQueueElement[Obj, int]]:
    obj_size = get_obj_size_bytes(obj, version=version, at=at, category=category)
    return QueueElement(CategoryQueueElement(obj=obj, category=category), version, obj_size, at)


def serializer(obj: "Obj") -> dict:
    return obj.__dict__


def deserializer(obj: dict) -> "Obj":
    return Obj(**obj)


def version_getter(obj: "Obj") -> int:
    return obj.num
