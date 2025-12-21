from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InitBackendCommand(_message.Message):
    __slots__ = ("meta",)
    META_FIELD_NUMBER: _ClassVar[int]
    meta: _any_pb2.Any
    def __init__(self, meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class InitBackendResponse(_message.Message):
    __slots__ = ("meta",)
    class InitBackendSynapseMeta(_message.Message):
        __slots__ = ("globalRoleName",)
        GLOBALROLENAME_FIELD_NUMBER: _ClassVar[int]
        globalRoleName: str
        def __init__(self, globalRoleName: _Optional[str] = ...) -> None: ...
    META_FIELD_NUMBER: _ClassVar[int]
    meta: _any_pb2.Any
    def __init__(self, meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class RemoveBackendCommand(_message.Message):
    __slots__ = ("meta",)
    META_FIELD_NUMBER: _ClassVar[int]
    meta: _any_pb2.Any
    def __init__(self, meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
