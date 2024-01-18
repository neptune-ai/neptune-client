from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class ProtobufFloatPointValueDto(_message.Message):
    __slots__ = ("timestamp_millis", "step", "value")
    TIMESTAMP_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp_millis: int
    step: float
    value: float
    def __init__(
        self, timestamp_millis: _Optional[int] = ..., step: _Optional[float] = ..., value: _Optional[float] = ...
    ) -> None: ...

class ProtobufFloatSeriesDto(_message.Message):
    __slots__ = ("values", "total_item_count")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[ProtobufFloatPointValueDto]
    total_item_count: int
    def __init__(
        self,
        values: _Optional[_Iterable[_Union[ProtobufFloatPointValueDto, _Mapping]]] = ...,
        total_item_count: _Optional[int] = ...,
    ) -> None: ...

class ProtobufStringPointValueDto(_message.Message):
    __slots__ = ("timestamp_millis", "step", "value")
    TIMESTAMP_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp_millis: int
    step: float
    value: str
    def __init__(
        self, timestamp_millis: _Optional[int] = ..., step: _Optional[float] = ..., value: _Optional[str] = ...
    ) -> None: ...

class ProtobufStringSeriesDto(_message.Message):
    __slots__ = ("values", "total_item_count")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[ProtobufStringPointValueDto]
    total_item_count: int
    def __init__(
        self,
        values: _Optional[_Iterable[_Union[ProtobufStringPointValueDto, _Mapping]]] = ...,
        total_item_count: _Optional[int] = ...,
    ) -> None: ...
