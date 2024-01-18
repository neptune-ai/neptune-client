import pytest
from google.protobuf.message import Message

from neptune.internal.proto.protobuf_api_series_dto_pb2 import (
    ProtobufFloatPointValueDto,
    ProtobufFloatSeriesDto,
    ProtobufStringPointValueDto,
    ProtobufStringSeriesDto,
)


@pytest.fixture
def float_point_value_dto() -> ProtobufFloatPointValueDto:
    return ProtobufFloatPointValueDto(
        timestamp_millis=1,
        step=2.0,
        value=3.0,
    )


@pytest.fixture
def float_series_dto(float_point_value_dto) -> ProtobufFloatSeriesDto:
    return ProtobufFloatSeriesDto(
        values=[
            float_point_value_dto,
            float_point_value_dto,
            float_point_value_dto,
        ],
        total_item_count=3,
    )


@pytest.fixture
def string_point_value_dto() -> ProtobufStringPointValueDto:
    return ProtobufStringPointValueDto(
        timestamp_millis=1,
        step=2.0,
        value="3",
    )


@pytest.fixture
def string_series_dto(string_point_value_dto) -> ProtobufStringSeriesDto:
    return ProtobufStringSeriesDto(
        values=[
            string_point_value_dto,
            string_point_value_dto,
            string_point_value_dto,
        ],
        total_item_count=3,
    )


@pytest.mark.parametrize(
    "obj_name", ["float_point_value_dto", "float_series_dto", "string_point_value_dto", "string_series_dto"]
)
def test_encode_decode(obj_name: str, request):
    obj: Message = request.getfixturevalue(obj_name)
    assert obj.FromString(obj.SerializeToString()) == obj
