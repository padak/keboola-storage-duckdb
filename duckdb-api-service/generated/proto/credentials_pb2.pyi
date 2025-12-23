from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GenericBackendCredentials(_message.Message):
    __slots__ = ("host", "principal", "secret", "port", "meta")
    class RedshiftCredentialsMeta(_message.Message):
        __slots__ = ("database",)
        DATABASE_FIELD_NUMBER: _ClassVar[int]
        database: str
        def __init__(self, database: _Optional[str] = ...) -> None: ...
    class TeradataCredentialsMeta(_message.Message):
        __slots__ = ("database",)
        DATABASE_FIELD_NUMBER: _ClassVar[int]
        database: str
        def __init__(self, database: _Optional[str] = ...) -> None: ...
    class SnowflakeCredentialsMeta(_message.Message):
        __slots__ = ("database", "warehouse", "workspaceStatementTimeoutSeconds", "tracingLevel")
        DATABASE_FIELD_NUMBER: _ClassVar[int]
        WAREHOUSE_FIELD_NUMBER: _ClassVar[int]
        WORKSPACESTATEMENTTIMEOUTSECONDS_FIELD_NUMBER: _ClassVar[int]
        TRACINGLEVEL_FIELD_NUMBER: _ClassVar[int]
        database: str
        warehouse: str
        workspaceStatementTimeoutSeconds: int
        tracingLevel: int
        def __init__(self, database: _Optional[str] = ..., warehouse: _Optional[str] = ..., workspaceStatementTimeoutSeconds: _Optional[int] = ..., tracingLevel: _Optional[int] = ...) -> None: ...
    class SynapseCredentialsMeta(_message.Message):
        __slots__ = ("database", "useManagedIdentity")
        DATABASE_FIELD_NUMBER: _ClassVar[int]
        USEMANAGEDIDENTITY_FIELD_NUMBER: _ClassVar[int]
        database: str
        useManagedIdentity: bool
        def __init__(self, database: _Optional[str] = ..., useManagedIdentity: bool = ...) -> None: ...
    class BigQueryCredentialsMeta(_message.Message):
        __slots__ = ("folder_id", "region")
        FOLDER_ID_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        folder_id: str
        region: str
        def __init__(self, folder_id: _Optional[str] = ..., region: _Optional[str] = ...) -> None: ...
    HOST_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SECRET_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    host: str
    principal: str
    secret: str
    port: int
    meta: _any_pb2.Any
    def __init__(self, host: _Optional[str] = ..., principal: _Optional[str] = ..., secret: _Optional[str] = ..., port: _Optional[int] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
