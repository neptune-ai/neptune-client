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
from mock import patch

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import (
    AssignInt,
    LogFloats,
    LogStrings,
    TrackFilesToArtifact,
    UploadFileContent,
)
from neptune.internal.operation_processors.batcher import Batcher
from neptune.internal.queue.disk_queue import QueueElement


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.neptune_backend.NeptuneBackend")
def test_empty_queue(backend, queue):
    # given
    queue.get.return_value = None

    # and
    batcher = Batcher(queue=queue, backend=backend)

    # when
    result = batcher.collect_batch()

    # then
    assert result is None


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_batch_preparation(backend, queue):
    # given
    record1 = QueueElement(AssignInt(["a"], 2501), 1, 1)
    record2 = QueueElement(TrackFilesToArtifact(["b"], "123", [("a/b/c", "d/e/f")]), 2, 1)
    record3 = QueueElement(UploadFileContent(["c"], "flac", "deadbeef"), 3, 1)

    # and
    queue.get.side_effect = (record1, record2, record3, None)

    # and
    batcher = Batcher(queue=queue, backend=backend)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [AssignInt(["a"], 2501)] == accumulated_operations.other_operations
    assert [UploadFileContent(["c"], "flac", "deadbeef")] == accumulated_operations.upload_operations
    assert accumulated_operations.artifact_operations == [TrackFilesToArtifact(["b"], "123", [("a/b/c", "d/e/f")])]
    assert accumulated_operations.errors == []
    assert 3 == accumulated_operations.source_operations_count
    assert 3 == version


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_batch_with_limited_attributes(backend, queue):
    # given
    record1 = QueueElement(AssignInt(["a"], 2501), 1, 1)
    record2 = QueueElement(TrackFilesToArtifact(["b"], "123", [("a/b/c", "d/e/f")]), 2, 1)
    record3 = QueueElement(UploadFileContent(["c"], "flac", "deadbeef"), 3, 1)

    # and
    queue.get.side_effect = (record1, record2, record3, None)

    # and
    batcher = Batcher(queue=queue, backend=backend, max_attributes_in_batch=2)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [AssignInt(["a"], 2501)] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [TrackFilesToArtifact(["b"], "123", [("a/b/c", "d/e/f")])] == accumulated_operations.artifact_operations
    assert [] == accumulated_operations.errors
    assert 2 == accumulated_operations.source_operations_count
    assert 2 == version


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_batch_with_limited_points(backend, queue):
    # given
    record1 = QueueElement(LogFloats(["a"], [1.0, 2.0]), 1, 1)
    record2 = QueueElement(LogFloats(["a"], [3.0]), 2, 1)
    record3 = QueueElement(LogFloats(["a"], [4.0, 5.0, 6.0]), 3, 1)

    # and
    queue.get.side_effect = (record1, record2, record3, None)

    # and
    batcher = Batcher(queue=queue, backend=backend, max_points_per_batch=3)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [LogFloats(["a"], [1.0, 2.0, 3.0])] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [] == accumulated_operations.artifact_operations
    assert [] == accumulated_operations.errors
    assert 1 == accumulated_operations.source_operations_count
    assert 2 == version


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_batch_with_limited_points_per_attribute(backend, queue):
    # given
    record1 = QueueElement(LogFloats(["a"], [1.0, 2.0]), 1, 1)
    record2 = QueueElement(LogFloats(["a"], [3.0]), 2, 1)
    record3 = QueueElement(LogFloats(["b"], [4.0, 5.0, 6.0]), 3, 1)
    record4 = QueueElement(LogFloats(["a"], [7.0]), 4, 1)

    # and
    queue.get.side_effect = (record1, record2, record3, record4, None)

    # and
    batcher = Batcher(queue=queue, backend=backend, max_points_per_attribute=3)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [
        LogFloats(["a"], [1.0, 2.0, 3.0]),
        LogFloats(["b"], [4.0, 5.0, 6.0]),
    ] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [] == accumulated_operations.artifact_operations
    assert [] == accumulated_operations.errors
    assert 2 == accumulated_operations.source_operations_count
    assert 3 == version


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_batch_errors(backend, queue):
    # given
    record1 = QueueElement(LogFloats(["a"], [1.0]), 1, 1)
    record2 = QueueElement(LogStrings(["a"], ["test"]), 2, 1)

    # and
    queue.get.side_effect = (record1, record2, None)

    # and
    batcher = Batcher(queue=queue, backend=backend, max_points_per_attribute=3)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [LogFloats(["a"], [1.0])] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [] == accumulated_operations.artifact_operations
    assert [
        MetadataInconsistency("Cannot perform LogStrings operation on a: Attribute is not a String Series")
    ] == accumulated_operations.errors
    assert 1 == accumulated_operations.source_operations_count
    assert 2 == version


@patch("neptune.internal.queue.disk_queue.DiskQueue")
@patch("neptune.internal.backends.factory.HostedNeptuneBackend")
def test_continue(backend, queue):
    # given
    record1 = QueueElement(LogFloats(["a"], [1.0, 2.0]), 1, 1)
    record2 = QueueElement(LogFloats(["a"], [3.0]), 2, 1)
    record3 = QueueElement(LogFloats(["a"], [4.0, 5.0]), 3, 1)

    # and
    queue.get.side_effect = (record1, record2, record3, None)

    # and
    batcher = Batcher(queue=queue, backend=backend, max_points_per_batch=3)

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [LogFloats(["a"], [1.0, 2.0, 3.0])] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [] == accumulated_operations.artifact_operations
    assert [] == accumulated_operations.errors
    assert 1 == accumulated_operations.source_operations_count
    assert 2 == version

    # when
    result = batcher.collect_batch()

    # then
    assert result is not None

    # and
    accumulated_operations, _, version = result
    assert [LogFloats(["a"], [4.0, 5.0])] == accumulated_operations.other_operations
    assert [] == accumulated_operations.upload_operations
    assert [] == accumulated_operations.artifact_operations
    assert [] == accumulated_operations.errors
    assert 1 == accumulated_operations.source_operations_count
    assert 3 == version

    # and
    assert 4 == len(queue.get.mock_calls)
