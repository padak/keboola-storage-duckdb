from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TimePartitioning(_message.Message):
    __slots__ = ("type", "expirationMs", "field")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONMS_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    type: str
    expirationMs: int
    field: str
    def __init__(self, type: _Optional[str] = ..., expirationMs: _Optional[int] = ..., field: _Optional[str] = ...) -> None: ...

class RangePartitioning(_message.Message):
    __slots__ = ("field", "range")
    class Range(_message.Message):
        __slots__ = ("start", "end", "interval")
        START_FIELD_NUMBER: _ClassVar[int]
        END_FIELD_NUMBER: _ClassVar[int]
        INTERVAL_FIELD_NUMBER: _ClassVar[int]
        start: int
        end: int
        interval: int
        def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., interval: _Optional[int] = ...) -> None: ...
    FIELD_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    field: str
    range: RangePartitioning.Range
    def __init__(self, field: _Optional[str] = ..., range: _Optional[_Union[RangePartitioning.Range, _Mapping]] = ...) -> None: ...

class Clustering(_message.Message):
    __slots__ = ("fields",)
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, fields: _Optional[_Iterable[str]] = ...) -> None: ...
