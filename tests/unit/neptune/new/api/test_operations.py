from datetime import datetime

from google.protobuf import timestamp_pb2
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as ProtoRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Step,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub import ingest_pb2

from neptune.api.operations import (
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInteger,
    AssignString,
    FloatValue,
    LogFloats,
    Run,
    RunOperation,
)


def test_assign_float():
    op = AssignFloat("path", 1.0)
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            assign={"path": Value(float64=1.0)},
        ),
    )


def test_assign_int():
    op = AssignInteger("path", 1)
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            assign={"path": Value(int64=1)},
        ),
    )


def test_assign_bool():
    op = AssignBool("path", True)
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            assign={"path": Value(bool=True)},
        ),
    )


def test_assign_string():
    op = AssignString("path", "value")
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            assign={"path": Value(string="value")},
        ),
    )


def test_assign_datetime():
    op = AssignDatetime("path", datetime(2021, 1, 1))
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            assign={"path": Value(timestamp=timestamp_pb2.Timestamp(seconds=int(datetime(2021, 1, 1).timestamp())))},
        ),
    )


def test_log_floats():
    op = LogFloats("path", [FloatValue(1, 1.0, 1)])
    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            step=Step(whole=1, micro=0),
            timestamp=timestamp_pb2.Timestamp(seconds=1),
            append={"path": Value(float64=1.0)},
        ),
    )


def test_run_creation():
    op = Run(datetime(2021, 1, 1), "run_id")

    run_op = RunOperation("project", "run_id", op)

    serialized = op.to_proto(run_op)

    assert serialized == ingest_pb2.RunOperation(
        project="project",
        run_id="run_id",
        create=ProtoRun(
            creation_time=timestamp_pb2.Timestamp(seconds=int(datetime(2021, 1, 1).timestamp())),
            run_id="run_id",
            family="run_id",
            experiment_id="run_id",
        ),
    )
