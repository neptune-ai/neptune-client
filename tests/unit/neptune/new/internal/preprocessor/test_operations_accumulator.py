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
import datetime

import pytest

from neptune.attributes import Integer as IntegerAttributeType
from neptune.exceptions import MetadataInconsistency
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import (
    AddStrings,
    AssignArtifact,
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    ClearArtifact,
    ClearFloatLog,
    ClearImageLog,
    ClearStringLog,
    ClearStringSet,
    ConfigFloatSeries,
    CopyAttribute,
    DeleteAttribute,
    DeleteFiles,
    LogFloats,
    LogImages,
    LogStrings,
    RemoveStrings,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.internal.preprocessor.operations_accumulator import OperationsAccumulator


def test_assign_float():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignFloat(path, 1.0),
        AssignFloat(path, 2.0),
        AssignFloat(path, 3.0),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignFloat(path, 3.0)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_assign_int():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignInt(path, 1),
        AssignInt(path, 2),
        AssignInt(path, 3),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignInt(path, 3)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_assign_bool():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignBool(path, False),
        AssignBool(path, True),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignBool(path, True)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_assign_string():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignString(path, "a"),
        AssignString(path, "b"),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignString(path, "b")] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_assign_datetime():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignDatetime(path, datetime.datetime(1879, 3, 14)),
        AssignDatetime(path, datetime.datetime(1846, 9, 23)),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignDatetime(path, datetime.datetime(1846, 9, 23))] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_log_floats():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogFloats(path, [1.0]),
        LogFloats(path, [2.0, 3.0]),
        LogFloats(path, [4.0, 5.0]),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [LogFloats(path, [1.0, 2.0, 3.0, 4.0, 5.0])] == accumulator.get_operations()
    assert 5 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_log_strings():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogStrings(path, ["a"]),
        LogStrings(path, ["b", "c"]),
        LogStrings(path, ["d", "e"]),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [LogStrings(path, ["a", "b", "c", "d", "e"])] == accumulator.get_operations()
    assert 5 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_log_images():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogImages(path, ["a"]),
        LogImages(path, ["b", "c"]),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [LogImages(path, ["a", "b", "c"])] == accumulator.get_operations()
    assert 3 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_clear_floats():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogFloats(path, [1.0]),
        LogFloats(path, [2.0, 3.0]),
        LogFloats(path, [4.0, 5.0]),
        ClearFloatLog(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [ClearFloatLog(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_clear_strings():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogStrings(path, ["a"]),
        LogStrings(path, ["b", "c"]),
        LogStrings(path, ["d", "e"]),
        ClearStringLog(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [ClearStringLog(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_clear_images():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        LogImages(path, ["a"]),
        LogImages(path, ["b", "c"]),
        ClearImageLog(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [ClearImageLog(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_config():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        ConfigFloatSeries(path, min=1.0, max=100.0, unit="%"),
        LogFloats(path, [1.0]),
        LogFloats(path, [2.0, 3.0]),
        LogFloats(path, [4.0, 5.0]),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [
        LogFloats(path, [1.0, 2.0, 3.0, 4.0, 5.0]),
        ConfigFloatSeries(path, min=1.0, max=100.0, unit="%"),
    ] == accumulator.get_operations()
    assert 5 == accumulator.get_append_count()
    assert 2 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_upload():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        UploadFile(path, "a"),
        UploadFile(path, "b"),
        UploadFileContent(path, "flac", "c"),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [UploadFileContent(path, "flac", "c")] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_string_sets():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AddStrings(path, {"xx", "y", "abc"}),
        AddStrings(path, {"zzz"}),
        RemoveStrings(path, {"y", "abc"}),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [
        AddStrings(path, {"xx", "y", "abc"}),
        AddStrings(path, {"zzz"}),
        RemoveStrings(path, {"y", "abc"}),
    ] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 3 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_clear_string_sets():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AddStrings(path, {"xx", "y", "abc"}),
        AddStrings(path, {"zzz"}),
        ClearStringSet(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [ClearStringSet(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_assign_artifact():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignArtifact(path, "a"),
        AssignArtifact(path, "b"),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignArtifact(path, "b")] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_clear_artifact():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignArtifact(path, "a"),
        AssignArtifact(path, "b"),
        ClearArtifact(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [ClearArtifact(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_artifact_track_files():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        TrackFilesToArtifact(path, "a", [("a/b/c", "d/e/f")]),
        TrackFilesToArtifact(path, "a", [("g/h/i", "j/k/l"), ("m", "n")]),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [
        TrackFilesToArtifact(path, "a", [("a/b/c", "d/e/f"), ("g/h/i", "j/k/l"), ("m", "n")])
    ] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_delete_float():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [
        AssignFloat(path, 1.0),
        AssignFloat(path, 2.0),
        AssignFloat(path, 3.0),
        DeleteAttribute(path),
        AssignFloat(path, 4.0),
        DeleteAttribute(path),
    ]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [AssignFloat(path, 3.0), DeleteAttribute(path)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 2 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_copy():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)

    # when
    with pytest.raises(MetadataInconsistency):
        CopyAttribute(path, "a", ContainerType.RUN, ["d", "e"], IntegerAttributeType).accept(accumulator)


def test_file_set():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [UploadFileSet(path, ["a", "b"], False), UploadFileSet(path, ["c"], False), DeleteFiles(path, {"b"})]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [
        UploadFileSet(path, ["a", "b"], False),
        UploadFileSet(path, ["c"], False),
        DeleteFiles(path, {"b"}),
    ] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 3 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()


def test_file_set_reset():
    # given
    path = ["a", "b", "c"]
    accumulator = OperationsAccumulator(path=path)
    operations = [UploadFileSet(path, ["a", "b"], False), UploadFileSet(path, ["c"], True)]

    # when
    for op in operations:
        op.accept(accumulator)

    # then
    assert [UploadFileSet(path, ["c"], True)] == accumulator.get_operations()
    assert 0 == accumulator.get_append_count()
    assert 1 == accumulator.get_op_count()
    assert [] == accumulator.get_errors()
