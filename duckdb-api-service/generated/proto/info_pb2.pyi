from google.protobuf import any_pb2 as _any_pb2
from proto.backend import bigQuery_pb2 as _bigQuery_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ObjectType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATABASE: _ClassVar[ObjectType]
    SCHEMA: _ClassVar[ObjectType]
    TABLE: _ClassVar[ObjectType]
    VIEW: _ClassVar[ObjectType]

class TableType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NORMAL: _ClassVar[TableType]
    EXTERNAL: _ClassVar[TableType]
DATABASE: ObjectType
SCHEMA: ObjectType
TABLE: ObjectType
VIEW: ObjectType
NORMAL: TableType
EXTERNAL: TableType

class ObjectInfoCommand(_message.Message):
    __slots__ = ("path", "expectedObjectType")
    PATH_FIELD_NUMBER: _ClassVar[int]
    EXPECTEDOBJECTTYPE_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    expectedObjectType: ObjectType
    def __init__(self, path: _Optional[_Iterable[str]] = ..., expectedObjectType: _Optional[_Union[ObjectType, str]] = ...) -> None: ...

class ObjectInfoResponse(_message.Message):
    __slots__ = ("path", "objectType", "databaseInfo", "schemaInfo", "viewInfo", "tableInfo")
    PATH_FIELD_NUMBER: _ClassVar[int]
    OBJECTTYPE_FIELD_NUMBER: _ClassVar[int]
    DATABASEINFO_FIELD_NUMBER: _ClassVar[int]
    SCHEMAINFO_FIELD_NUMBER: _ClassVar[int]
    VIEWINFO_FIELD_NUMBER: _ClassVar[int]
    TABLEINFO_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    objectType: ObjectType
    databaseInfo: DatabaseInfo
    schemaInfo: SchemaInfo
    viewInfo: ViewInfo
    tableInfo: TableInfo
    def __init__(self, path: _Optional[_Iterable[str]] = ..., objectType: _Optional[_Union[ObjectType, str]] = ..., databaseInfo: _Optional[_Union[DatabaseInfo, _Mapping]] = ..., schemaInfo: _Optional[_Union[SchemaInfo, _Mapping]] = ..., viewInfo: _Optional[_Union[ViewInfo, _Mapping]] = ..., tableInfo: _Optional[_Union[TableInfo, _Mapping]] = ...) -> None: ...

class ObjectInfo(_message.Message):
    __slots__ = ("objectName", "objectType")
    OBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    OBJECTTYPE_FIELD_NUMBER: _ClassVar[int]
    objectName: str
    objectType: ObjectType
    def __init__(self, objectName: _Optional[str] = ..., objectType: _Optional[_Union[ObjectType, str]] = ...) -> None: ...

class DatabaseInfo(_message.Message):
    __slots__ = ("objects",)
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[ObjectInfo]
    def __init__(self, objects: _Optional[_Iterable[_Union[ObjectInfo, _Mapping]]] = ...) -> None: ...

class SchemaInfo(_message.Message):
    __slots__ = ("objects",)
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[ObjectInfo]
    def __init__(self, objects: _Optional[_Iterable[_Union[ObjectInfo, _Mapping]]] = ...) -> None: ...

class ViewInfo(_message.Message):
    __slots__ = ("path", "viewName", "columns", "primaryKeysNames", "rowsCount", "meta")
    PATH_FIELD_NUMBER: _ClassVar[int]
    VIEWNAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    PRIMARYKEYSNAMES_FIELD_NUMBER: _ClassVar[int]
    ROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    viewName: str
    columns: _containers.RepeatedCompositeFieldContainer[TableInfo.TableColumn]
    primaryKeysNames: _containers.RepeatedScalarFieldContainer[str]
    rowsCount: int
    meta: _any_pb2.Any
    def __init__(self, path: _Optional[_Iterable[str]] = ..., viewName: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableInfo.TableColumn, _Mapping]]] = ..., primaryKeysNames: _Optional[_Iterable[str]] = ..., rowsCount: _Optional[int] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class TableInfo(_message.Message):
    __slots__ = ("path", "tableName", "columns", "primaryKeysNames", "rowsCount", "sizeBytes", "tableType", "meta")
    class TableColumn(_message.Message):
        __slots__ = ("name", "type", "length", "nullable", "default", "meta")
        class TeradataTableColumnMeta(_message.Message):
            __slots__ = ("isLatin",)
            ISLATIN_FIELD_NUMBER: _ClassVar[int]
            isLatin: bool
            def __init__(self, isLatin: bool = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        NULLABLE_FIELD_NUMBER: _ClassVar[int]
        DEFAULT_FIELD_NUMBER: _ClassVar[int]
        META_FIELD_NUMBER: _ClassVar[int]
        name: str
        type: str
        length: str
        nullable: bool
        default: str
        meta: _any_pb2.Any
        def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ..., length: _Optional[str] = ..., nullable: bool = ..., default: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
    class BigQueryTableMeta(_message.Message):
        __slots__ = ("timePartitioning", "rangePartitioning", "requirePartitionFilter", "clustering", "partitions")
        class Partition(_message.Message):
            __slots__ = ("partition_id", "rowsNumber", "lastModifiedTime", "storageTier")
            PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
            ROWSNUMBER_FIELD_NUMBER: _ClassVar[int]
            LASTMODIFIEDTIME_FIELD_NUMBER: _ClassVar[int]
            STORAGETIER_FIELD_NUMBER: _ClassVar[int]
            partition_id: str
            rowsNumber: str
            lastModifiedTime: str
            storageTier: str
            def __init__(self, partition_id: _Optional[str] = ..., rowsNumber: _Optional[str] = ..., lastModifiedTime: _Optional[str] = ..., storageTier: _Optional[str] = ...) -> None: ...
        TIMEPARTITIONING_FIELD_NUMBER: _ClassVar[int]
        RANGEPARTITIONING_FIELD_NUMBER: _ClassVar[int]
        REQUIREPARTITIONFILTER_FIELD_NUMBER: _ClassVar[int]
        CLUSTERING_FIELD_NUMBER: _ClassVar[int]
        PARTITIONS_FIELD_NUMBER: _ClassVar[int]
        timePartitioning: _bigQuery_pb2.TimePartitioning
        rangePartitioning: _bigQuery_pb2.RangePartitioning
        requirePartitionFilter: bool
        clustering: _bigQuery_pb2.Clustering
        partitions: _containers.RepeatedCompositeFieldContainer[TableInfo.BigQueryTableMeta.Partition]
        def __init__(self, timePartitioning: _Optional[_Union[_bigQuery_pb2.TimePartitioning, _Mapping]] = ..., rangePartitioning: _Optional[_Union[_bigQuery_pb2.RangePartitioning, _Mapping]] = ..., requirePartitionFilter: bool = ..., clustering: _Optional[_Union[_bigQuery_pb2.Clustering, _Mapping]] = ..., partitions: _Optional[_Iterable[_Union[TableInfo.BigQueryTableMeta.Partition, _Mapping]]] = ...) -> None: ...
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    PRIMARYKEYSNAMES_FIELD_NUMBER: _ClassVar[int]
    ROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    SIZEBYTES_FIELD_NUMBER: _ClassVar[int]
    TABLETYPE_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    columns: _containers.RepeatedCompositeFieldContainer[TableInfo.TableColumn]
    primaryKeysNames: _containers.RepeatedScalarFieldContainer[str]
    rowsCount: int
    sizeBytes: int
    tableType: TableType
    meta: _any_pb2.Any
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableInfo.TableColumn, _Mapping]]] = ..., primaryKeysNames: _Optional[_Iterable[str]] = ..., rowsCount: _Optional[int] = ..., sizeBytes: _Optional[int] = ..., tableType: _Optional[_Union[TableType, str]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
