from google.protobuf import any_pb2 as _any_pb2
from proto import table_pb2 as _table_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateWorkspaceCommand(_message.Message):
    __slots__ = ("stackPrefix", "projectId", "workspaceId", "branchId", "isBranchDefault", "projectUserName", "projectRoleName", "projectReadOnlyRoleName", "devBranchReadOnlyRoleName", "meta")
    class CreateWorkspaceTeradataMeta(_message.Message):
        __slots__ = ("permSpace", "spoolSpace")
        PERMSPACE_FIELD_NUMBER: _ClassVar[int]
        SPOOLSPACE_FIELD_NUMBER: _ClassVar[int]
        permSpace: str
        spoolSpace: str
        def __init__(self, permSpace: _Optional[str] = ..., spoolSpace: _Optional[str] = ...) -> None: ...
    class CreateWorkspaceBigqueryMeta(_message.Message):
        __slots__ = ("region",)
        REGION_FIELD_NUMBER: _ClassVar[int]
        region: str
        def __init__(self, region: _Optional[str] = ...) -> None: ...
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    PROJECTID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEID_FIELD_NUMBER: _ClassVar[int]
    BRANCHID_FIELD_NUMBER: _ClassVar[int]
    ISBRANCHDEFAULT_FIELD_NUMBER: _ClassVar[int]
    PROJECTUSERNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    DEVBRANCHREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    projectId: str
    workspaceId: str
    branchId: str
    isBranchDefault: bool
    projectUserName: str
    projectRoleName: str
    projectReadOnlyRoleName: str
    devBranchReadOnlyRoleName: str
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., projectId: _Optional[str] = ..., workspaceId: _Optional[str] = ..., branchId: _Optional[str] = ..., isBranchDefault: bool = ..., projectUserName: _Optional[str] = ..., projectRoleName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., devBranchReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class CreateWorkspaceResponse(_message.Message):
    __slots__ = ("workspaceUserName", "workspaceRoleName", "workspacePassword", "workspaceObjectName")
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEROLENAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEPASSWORD_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    workspaceRoleName: str
    workspacePassword: str
    workspaceObjectName: str
    def __init__(self, workspaceUserName: _Optional[str] = ..., workspaceRoleName: _Optional[str] = ..., workspacePassword: _Optional[str] = ..., workspaceObjectName: _Optional[str] = ...) -> None: ...

class DropWorkspaceCommand(_message.Message):
    __slots__ = ("workspaceUserName", "workspaceRoleName", "workspaceObjectName", "isCascade")
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEROLENAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    ISCASCADE_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    workspaceRoleName: str
    workspaceObjectName: str
    isCascade: bool
    def __init__(self, workspaceUserName: _Optional[str] = ..., workspaceRoleName: _Optional[str] = ..., workspaceObjectName: _Optional[str] = ..., isCascade: bool = ...) -> None: ...

class ClearWorkspaceCommand(_message.Message):
    __slots__ = ("workspaceObjectName", "ignoreErrors", "objectsToPreserve")
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    IGNOREERRORS_FIELD_NUMBER: _ClassVar[int]
    OBJECTSTOPRESERVE_FIELD_NUMBER: _ClassVar[int]
    workspaceObjectName: str
    ignoreErrors: bool
    objectsToPreserve: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, workspaceObjectName: _Optional[str] = ..., ignoreErrors: bool = ..., objectsToPreserve: _Optional[_Iterable[str]] = ...) -> None: ...

class ResetWorkspacePasswordCommand(_message.Message):
    __slots__ = ("workspaceUserName",)
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    def __init__(self, workspaceUserName: _Optional[str] = ...) -> None: ...

class ResetWorkspacePasswordResponse(_message.Message):
    __slots__ = ("workspaceUserName", "workspacePassword")
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEPASSWORD_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    workspacePassword: str
    def __init__(self, workspaceUserName: _Optional[str] = ..., workspacePassword: _Optional[str] = ...) -> None: ...

class DropWorkspaceObjectCommand(_message.Message):
    __slots__ = ("workspaceObjectName", "objectNameToDrop", "ignoreIfNotExists")
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    OBJECTNAMETODROP_FIELD_NUMBER: _ClassVar[int]
    IGNOREIFNOTEXISTS_FIELD_NUMBER: _ClassVar[int]
    workspaceObjectName: str
    objectNameToDrop: str
    ignoreIfNotExists: bool
    def __init__(self, workspaceObjectName: _Optional[str] = ..., objectNameToDrop: _Optional[str] = ..., ignoreIfNotExists: bool = ...) -> None: ...

class GrantWorkspaceAccessToProjectCommand(_message.Message):
    __slots__ = ("workspaceUserName", "workspaceRoleName", "workspaceObjectName", "projectUserName", "projectRoleName")
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEROLENAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTUSERNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    workspaceRoleName: str
    workspaceObjectName: str
    projectUserName: str
    projectRoleName: str
    def __init__(self, workspaceUserName: _Optional[str] = ..., workspaceRoleName: _Optional[str] = ..., workspaceObjectName: _Optional[str] = ..., projectUserName: _Optional[str] = ..., projectRoleName: _Optional[str] = ...) -> None: ...

class RevokeWorkspaceAccessToProjectCommand(_message.Message):
    __slots__ = ("workspaceUserName", "workspaceRoleName", "workspaceObjectName", "projectUserName", "projectRoleName")
    WORKSPACEUSERNAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEROLENAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACEOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTUSERNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    workspaceUserName: str
    workspaceRoleName: str
    workspaceObjectName: str
    projectUserName: str
    projectRoleName: str
    def __init__(self, workspaceUserName: _Optional[str] = ..., workspaceRoleName: _Optional[str] = ..., workspaceObjectName: _Optional[str] = ..., projectUserName: _Optional[str] = ..., projectRoleName: _Optional[str] = ...) -> None: ...

class LoadTableToWorkspaceCommand(_message.Message):
    __slots__ = ("source", "destination", "importOptions")
    class SourceTableMapping(_message.Message):
        __slots__ = ("path", "tableName", "whereFilters", "limit", "columnMappings")
        class ColumnMapping(_message.Message):
            __slots__ = ("sourceColumnName", "destinationColumnName")
            SOURCECOLUMNNAME_FIELD_NUMBER: _ClassVar[int]
            DESTINATIONCOLUMNNAME_FIELD_NUMBER: _ClassVar[int]
            sourceColumnName: str
            destinationColumnName: str
            def __init__(self, sourceColumnName: _Optional[str] = ..., destinationColumnName: _Optional[str] = ...) -> None: ...
        PATH_FIELD_NUMBER: _ClassVar[int]
        TABLENAME_FIELD_NUMBER: _ClassVar[int]
        WHEREFILTERS_FIELD_NUMBER: _ClassVar[int]
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        COLUMNMAPPINGS_FIELD_NUMBER: _ClassVar[int]
        path: _containers.RepeatedScalarFieldContainer[str]
        tableName: str
        whereFilters: _containers.RepeatedCompositeFieldContainer[_table_pb2.ImportExportShared.TableWhereFilter]
        limit: int
        columnMappings: _containers.RepeatedCompositeFieldContainer[LoadTableToWorkspaceCommand.SourceTableMapping.ColumnMapping]
        def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., whereFilters: _Optional[_Iterable[_Union[_table_pb2.ImportExportShared.TableWhereFilter, _Mapping]]] = ..., limit: _Optional[int] = ..., columnMappings: _Optional[_Iterable[_Union[LoadTableToWorkspaceCommand.SourceTableMapping.ColumnMapping, _Mapping]]] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    IMPORTOPTIONS_FIELD_NUMBER: _ClassVar[int]
    source: LoadTableToWorkspaceCommand.SourceTableMapping
    destination: _table_pb2.ImportExportShared.Table
    importOptions: _table_pb2.ImportExportShared.ImportOptions
    def __init__(self, source: _Optional[_Union[LoadTableToWorkspaceCommand.SourceTableMapping, _Mapping]] = ..., destination: _Optional[_Union[_table_pb2.ImportExportShared.Table, _Mapping]] = ..., importOptions: _Optional[_Union[_table_pb2.ImportExportShared.ImportOptions, _Mapping]] = ...) -> None: ...
