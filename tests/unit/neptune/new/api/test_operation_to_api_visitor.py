from datetime import (
    datetime,
    timezone,
)

import pytest
from google.protobuf import timestamp_pb2
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Run,
    Step,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation as ProtoRunOperation

import neptune.core.operations.operation as core_operations
from neptune.api.operation_to_api import OperationToApiVisitor
from neptune.api.operations import RunOperation


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
            assign={f"{path}": value},
        ),
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
            append={f"{path}": value},
        ),
        api_key=b"",
    )


def test_operation_to_api_visitor_run_creation():
    # given
    visitor = OperationToApiVisitor()

    # and
    created_at = datetime(2021, 1, 1, tzinfo=timezone.utc)
    expected_creation_time = timestamp_pb2.Timestamp()
    expected_creation_time.FromDatetime(created_at)

    # and
    run_creation = core_operations.CreateRun(created_at=created_at.timestamp(), custom_id="custom_id")

    # when
    api_operation = run_creation.accept(visitor)

    run_op = RunOperation("project", "run_id", operation=api_operation)

    res = run_op.to_proto()

    expected_create = Run(
        run_id="custom_id",
        experiment_id="custom_id",
        family="custom_id",
        creation_time=expected_creation_time,
    )

    expected = ProtoRunOperation(
        project="project", run_id="run_id", create_missing_project=False, create=expected_create, update=None
    )

    assert res == expected


@pytest.mark.parametrize(
    ["core_operation", "expected_proto_run_operation"],
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
    core_operation: core_operations.Operation,
    expected_proto_run_operation: ProtoRunOperation,
    visitor: OperationToApiVisitor,
):
    api_op = core_operation.accept(visitor)

    run_op = RunOperation("project", "run_id", operation=api_op)

    res = run_op.to_proto()

    assert res == expected_proto_run_operation
