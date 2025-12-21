from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateProjectCommand(_message.Message):
    __slots__ = ("stackPrefix", "projectId", "dataRetentionTime", "fileStorage", "meta")
    class FileStorageType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        S3: _ClassVar[CreateProjectCommand.FileStorageType]
        ABS: _ClassVar[CreateProjectCommand.FileStorageType]
        GCS: _ClassVar[CreateProjectCommand.FileStorageType]
    S3: CreateProjectCommand.FileStorageType
    ABS: CreateProjectCommand.FileStorageType
    GCS: CreateProjectCommand.FileStorageType
    class CreateProjectTeradataMeta(_message.Message):
        __slots__ = ("rootDatabase", "permSpace", "spoolSpace")
        ROOTDATABASE_FIELD_NUMBER: _ClassVar[int]
        PERMSPACE_FIELD_NUMBER: _ClassVar[int]
        SPOOLSPACE_FIELD_NUMBER: _ClassVar[int]
        rootDatabase: str
        permSpace: str
        spoolSpace: str
        def __init__(self, rootDatabase: _Optional[str] = ..., permSpace: _Optional[str] = ..., spoolSpace: _Optional[str] = ...) -> None: ...
    class CreateProjectBigqueryMeta(_message.Message):
        __slots__ = ("gcsFileBucketName", "region")
        GCSFILEBUCKETNAME_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        gcsFileBucketName: str
        region: str
        def __init__(self, gcsFileBucketName: _Optional[str] = ..., region: _Optional[str] = ...) -> None: ...
    class CreateProjectSnowflakeMeta(_message.Message):
        __slots__ = ("storageIntegrationName", "projectUserLoginType", "projectUserPublicKey", "setupDynamicBackends", "defaultWarehouseToGrant", "additionalWarehousesToGrant")
        STORAGEINTEGRATIONNAME_FIELD_NUMBER: _ClassVar[int]
        PROJECTUSERLOGINTYPE_FIELD_NUMBER: _ClassVar[int]
        PROJECTUSERPUBLICKEY_FIELD_NUMBER: _ClassVar[int]
        SETUPDYNAMICBACKENDS_FIELD_NUMBER: _ClassVar[int]
        DEFAULTWAREHOUSETOGRANT_FIELD_NUMBER: _ClassVar[int]
        ADDITIONALWAREHOUSESTOGRANT_FIELD_NUMBER: _ClassVar[int]
        storageIntegrationName: str
        projectUserLoginType: str
        projectUserPublicKey: str
        setupDynamicBackends: bool
        defaultWarehouseToGrant: str
        additionalWarehousesToGrant: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, storageIntegrationName: _Optional[str] = ..., projectUserLoginType: _Optional[str] = ..., projectUserPublicKey: _Optional[str] = ..., setupDynamicBackends: bool = ..., defaultWarehouseToGrant: _Optional[str] = ..., additionalWarehousesToGrant: _Optional[_Iterable[str]] = ...) -> None: ...
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    PROJECTID_FIELD_NUMBER: _ClassVar[int]
    DATARETENTIONTIME_FIELD_NUMBER: _ClassVar[int]
    FILESTORAGE_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    projectId: str
    dataRetentionTime: int
    fileStorage: CreateProjectCommand.FileStorageType
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., projectId: _Optional[str] = ..., dataRetentionTime: _Optional[int] = ..., fileStorage: _Optional[_Union[CreateProjectCommand.FileStorageType, str]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class CreateProjectResponse(_message.Message):
    __slots__ = ("projectUserName", "projectRoleName", "projectPassword", "projectReadOnlyRoleName", "projectDatabaseName", "meta")
    class CreateProjectSnowflakeMeta(_message.Message):
        __slots__ = ("isNetworkPolicySet",)
        ISNETWORKPOLICYSET_FIELD_NUMBER: _ClassVar[int]
        isNetworkPolicySet: bool
        def __init__(self, isNetworkPolicySet: bool = ...) -> None: ...
    PROJECTUSERNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTPASSWORD_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTDATABASENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    projectUserName: str
    projectRoleName: str
    projectPassword: str
    projectReadOnlyRoleName: str
    projectDatabaseName: str
    meta: _any_pb2.Any
    def __init__(self, projectUserName: _Optional[str] = ..., projectRoleName: _Optional[str] = ..., projectPassword: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., projectDatabaseName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class UpdateProjectCommand(_message.Message):
    __slots__ = ("projectId", "region", "timezone")
    PROJECTID_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    projectId: str
    region: str
    timezone: str
    def __init__(self, projectId: _Optional[str] = ..., region: _Optional[str] = ..., timezone: _Optional[str] = ...) -> None: ...

class DropProjectCommand(_message.Message):
    __slots__ = ("projectUserName", "projectRoleName", "readOnlyRoleName", "projectDatabaseName", "meta")
    class DropProjectBigqueryMeta(_message.Message):
        __slots__ = ("gcsFileBucketName", "region")
        GCSFILEBUCKETNAME_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        gcsFileBucketName: str
        region: str
        def __init__(self, gcsFileBucketName: _Optional[str] = ..., region: _Optional[str] = ...) -> None: ...
    PROJECTUSERNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    READONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTDATABASENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    projectUserName: str
    projectRoleName: str
    readOnlyRoleName: str
    projectDatabaseName: str
    meta: _any_pb2.Any
    def __init__(self, projectUserName: _Optional[str] = ..., projectRoleName: _Optional[str] = ..., readOnlyRoleName: _Optional[str] = ..., projectDatabaseName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class CreateDevBranchCommand(_message.Message):
    __slots__ = ("stackPrefix", "projectId", "branchId", "projectRoleName", "projectReadOnlyRoleName", "meta")
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    PROJECTID_FIELD_NUMBER: _ClassVar[int]
    BRANCHID_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    projectId: str
    branchId: str
    projectRoleName: str
    projectReadOnlyRoleName: str
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., projectId: _Optional[str] = ..., branchId: _Optional[str] = ..., projectRoleName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class CreateDevBranchResponse(_message.Message):
    __slots__ = ("devBranchReadOnlyRoleName",)
    DEVBRANCHREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    devBranchReadOnlyRoleName: str
    def __init__(self, devBranchReadOnlyRoleName: _Optional[str] = ...) -> None: ...

class DropDevBranchCommand(_message.Message):
    __slots__ = ("devBranchReadOnlyRoleName",)
    DEVBRANCHREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    devBranchReadOnlyRoleName: str
    def __init__(self, devBranchReadOnlyRoleName: _Optional[str] = ...) -> None: ...
