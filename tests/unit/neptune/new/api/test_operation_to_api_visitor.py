from datetime import datetime

import pytest
from google.protobuf import timestamp_pb2

import neptune.core.operations.operation as core_operations
from neptune.api.operation_to_api import OperationToApiVisitor
from neptune.api.operations import RunOperation
from neptune.api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Run,
    Step,
    UpdateRunSnapshot,
    Value,
)
from neptune.api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation as ProtoRunOperation


@pytest.fixture(scope="module")
def visitor() -> OperationToApiVisitor:
    return OperationToApiVisitor()


def build_expected_atom_proto_run_operation(path: str, value: Value) -> ProtoRunOperation:
    return ProtoRunOperation(
        project="project",
        run_id="run_id",
        create_missing_project=False,
        create=None,
        update=UpdateRunSnapshot(
            assign={
                "path": Value(string=path),
                "value": value,
            },
        ),
        api_key=b"",
    )


def build_expected_series_proto_run_operation(path: str, value: Value) -> ProtoRunOperation:
    return ProtoRunOperation(
        project="project",
        run_id="run_id",
        create_missing_project=False,
        create=None,
        update=UpdateRunSnapshot(
            step=Step(whole=1, micro=0),
            timestamp=timestamp_pb2.Timestamp(seconds=1),
            append={
                "path": Value(string=path),
                "value": value,
            },
        ),
        api_key=b"",
    )


def test_operation_to_api_visitor_run_creation():
    visitor = OperationToApiVisitor()
    created_at = datetime(2021, 1, 1)
    run_creation = core_operations.RunCreation(created_at, "custom_id")

    api_op = run_creation.accept(visitor)

    run_op = RunOperation("project", "run_id", operation=api_op)

    res = run_op.to_proto()

    expected_create = Run(
        run_id="custom_id",
        creation_time=timestamp_pb2.Timestamp(seconds=int(created_at.timestamp())),
    )

    expected = ProtoRunOperation(
        project="project",
        run_id="run_id",
        create_missing_project=False,
        create=expected_create,
        update=None,
        api_key=b"",
    )

    assert res == expected


@pytest.mark.parametrize(
    ["operation", "expected_proto_run_operation"],
    [
        (
            core_operations.AssignInt(["path"], 1),
            build_expected_atom_proto_run_operation("path", Value(int64=1)),
        ),
        (
            core_operations.AssignFloat(["path"], 1.0),
            build_expected_atom_proto_run_operation("path", Value(float64=1.0)),
        ),
        (
            core_operations.AssignBool(["path"], True),
            build_expected_atom_proto_run_operation("path", Value(bool=True)),
        ),
        (
            core_operations.AssignString(["path"], "value"),
            build_expected_atom_proto_run_operation("path", Value(string="value")),
        ),
        (
            core_operations.AssignDatetime(["path"], datetime(2021, 1, 1)),
            build_expected_atom_proto_run_operation(
                "path", Value(timestamp=timestamp_pb2.Timestamp(seconds=int(datetime(2021, 1, 1).timestamp())))
            ),
        ),
        (
            core_operations.LogFloats(["path"], [core_operations.LogSeriesValue(1.0, ts=1, step=1)]),
            build_expected_series_proto_run_operation("path", Value(float64=1.0)),
        ),
    ],
)
def test_api_to_operation_visitor(
    operation: core_operations.Operation,
    expected_proto_run_operation: ProtoRunOperation,
    visitor: OperationToApiVisitor,
):
    api_op = operation.accept(visitor)

    run_op = RunOperation("project", "run_id", operation=api_op)

    res = run_op.to_proto()

    assert res == expected_proto_run_operation
