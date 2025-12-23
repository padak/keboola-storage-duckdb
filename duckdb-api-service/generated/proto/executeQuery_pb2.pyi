from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExecuteQueryCommand(_message.Message):
    __slots__ = ("pathRestriction", "timeout", "query", "snowflakeRole", "bigQueryServiceAccount")
    class SnowflakeRole(_message.Message):
        __slots__ = ("roleName",)
        ROLENAME_FIELD_NUMBER: _ClassVar[int]
        roleName: str
        def __init__(self, roleName: _Optional[str] = ...) -> None: ...
    class BigQueryServiceAccount(_message.Message):
        __slots__ = ("serviceAccountEmail", "projectId")
        SERVICEACCOUNTEMAIL_FIELD_NUMBER: _ClassVar[int]
        PROJECTID_FIELD_NUMBER: _ClassVar[int]
        serviceAccountEmail: str
        projectId: str
        def __init__(self, serviceAccountEmail: _Optional[str] = ..., projectId: _Optional[str] = ...) -> None: ...
    PATHRESTRICTION_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SNOWFLAKEROLE_FIELD_NUMBER: _ClassVar[int]
    BIGQUERYSERVICEACCOUNT_FIELD_NUMBER: _ClassVar[int]
    pathRestriction: _containers.RepeatedScalarFieldContainer[str]
    timeout: int
    query: str
    snowflakeRole: ExecuteQueryCommand.SnowflakeRole
    bigQueryServiceAccount: ExecuteQueryCommand.BigQueryServiceAccount
    def __init__(self, pathRestriction: _Optional[_Iterable[str]] = ..., timeout: _Optional[int] = ..., query: _Optional[str] = ..., snowflakeRole: _Optional[_Union[ExecuteQueryCommand.SnowflakeRole, _Mapping]] = ..., bigQueryServiceAccount: _Optional[_Union[ExecuteQueryCommand.BigQueryServiceAccount, _Mapping]] = ...) -> None: ...

class ExecuteQueryResponse(_message.Message):
    __slots__ = ("status", "data", "message")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        Success: _ClassVar[ExecuteQueryResponse.Status]
        Error: _ClassVar[ExecuteQueryResponse.Status]
    Success: ExecuteQueryResponse.Status
    Error: ExecuteQueryResponse.Status
    class Data(_message.Message):
        __slots__ = ("columns", "rows")
        class Row(_message.Message):
            __slots__ = ("fields",)
            class FieldsEntry(_message.Message):
                __slots__ = ("key", "value")
                KEY_FIELD_NUMBER: _ClassVar[int]
                VALUE_FIELD_NUMBER: _ClassVar[int]
                key: str
                value: str
                def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
            FIELDS_FIELD_NUMBER: _ClassVar[int]
            fields: _containers.ScalarMap[str, str]
            def __init__(self, fields: _Optional[_Mapping[str, str]] = ...) -> None: ...
        COLUMNS_FIELD_NUMBER: _ClassVar[int]
        ROWS_FIELD_NUMBER: _ClassVar[int]
        columns: _containers.RepeatedScalarFieldContainer[str]
        rows: _containers.RepeatedCompositeFieldContainer[ExecuteQueryResponse.Data.Row]
        def __init__(self, columns: _Optional[_Iterable[str]] = ..., rows: _Optional[_Iterable[_Union[ExecuteQueryResponse.Data.Row, _Mapping]]] = ...) -> None: ...
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: ExecuteQueryResponse.Status
    data: ExecuteQueryResponse.Data
    message: str
    def __init__(self, status: _Optional[_Union[ExecuteQueryResponse.Status, str]] = ..., data: _Optional[_Union[ExecuteQueryResponse.Data, _Mapping]] = ..., message: _Optional[str] = ...) -> None: ...
