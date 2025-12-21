from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DriverRequest(_message.Message):
    __slots__ = ("credentials", "command", "features", "runtimeOptions")
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    RUNTIMEOPTIONS_FIELD_NUMBER: _ClassVar[int]
    credentials: _any_pb2.Any
    command: _any_pb2.Any
    features: _containers.RepeatedScalarFieldContainer[str]
    runtimeOptions: RuntimeOptions
    def __init__(self, credentials: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., command: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., features: _Optional[_Iterable[str]] = ..., runtimeOptions: _Optional[_Union[RuntimeOptions, _Mapping]] = ...) -> None: ...

class RuntimeOptions(_message.Message):
    __slots__ = ("runId", "queryTags", "meta")
    class QueryTagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RUNID_FIELD_NUMBER: _ClassVar[int]
    QUERYTAGS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    runId: str
    queryTags: _containers.ScalarMap[str, str]
    meta: _any_pb2.Any
    def __init__(self, runId: _Optional[str] = ..., queryTags: _Optional[_Mapping[str, str]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class LogMessage(_message.Message):
    __slots__ = ("level", "message", "context")
    class Level(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        Emergency: _ClassVar[LogMessage.Level]
        Alert: _ClassVar[LogMessage.Level]
        Critical: _ClassVar[LogMessage.Level]
        Error: _ClassVar[LogMessage.Level]
        Warning: _ClassVar[LogMessage.Level]
        Notice: _ClassVar[LogMessage.Level]
        Informational: _ClassVar[LogMessage.Level]
        Debug: _ClassVar[LogMessage.Level]
    Emergency: LogMessage.Level
    Alert: LogMessage.Level
    Critical: LogMessage.Level
    Error: LogMessage.Level
    Warning: LogMessage.Level
    Notice: LogMessage.Level
    Informational: LogMessage.Level
    Debug: LogMessage.Level
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    level: LogMessage.Level
    message: str
    context: _any_pb2.Any
    def __init__(self, level: _Optional[_Union[LogMessage.Level, str]] = ..., message: _Optional[str] = ..., context: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class DriverResponse(_message.Message):
    __slots__ = ("commandResponse", "messages")
    COMMANDRESPONSE_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    commandResponse: _any_pb2.Any
    messages: _containers.RepeatedCompositeFieldContainer[LogMessage]
    def __init__(self, commandResponse: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., messages: _Optional[_Iterable[_Union[LogMessage, _Mapping]]] = ...) -> None: ...
