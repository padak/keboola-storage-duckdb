from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from proto import info_pb2 as _info_pb2
from proto.backend import bigQuery_pb2 as _bigQuery_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TableColumnShared(_message.Message):
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

class CreateTableCommand(_message.Message):
    __slots__ = ("path", "tableName", "columns", "primaryKeysNames", "meta")
    class SynapseTableMeta(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    class BigQueryTableMeta(_message.Message):
        __slots__ = ("timePartitioning", "rangePartitioning", "requirePartitionFilter", "clustering")
        TIMEPARTITIONING_FIELD_NUMBER: _ClassVar[int]
        RANGEPARTITIONING_FIELD_NUMBER: _ClassVar[int]
        REQUIREPARTITIONFILTER_FIELD_NUMBER: _ClassVar[int]
        CLUSTERING_FIELD_NUMBER: _ClassVar[int]
        timePartitioning: _bigQuery_pb2.TimePartitioning
        rangePartitioning: _bigQuery_pb2.RangePartitioning
        requirePartitionFilter: bool
        clustering: _bigQuery_pb2.Clustering
        def __init__(self, timePartitioning: _Optional[_Union[_bigQuery_pb2.TimePartitioning, _Mapping]] = ..., rangePartitioning: _Optional[_Union[_bigQuery_pb2.RangePartitioning, _Mapping]] = ..., requirePartitionFilter: bool = ..., clustering: _Optional[_Union[_bigQuery_pb2.Clustering, _Mapping]] = ...) -> None: ...
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    PRIMARYKEYSNAMES_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    columns: _containers.RepeatedCompositeFieldContainer[TableColumnShared]
    primaryKeysNames: _containers.RepeatedScalarFieldContainer[str]
    meta: _any_pb2.Any
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableColumnShared, _Mapping]]] = ..., primaryKeysNames: _Optional[_Iterable[str]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class DropTableCommand(_message.Message):
    __slots__ = ("path", "tableName", "ignoreErrors")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    IGNOREERRORS_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    ignoreErrors: bool
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., ignoreErrors: bool = ...) -> None: ...

class AddColumnCommand(_message.Message):
    __slots__ = ("path", "tableName", "columnDefinition")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNDEFINITION_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    columnDefinition: TableColumnShared
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., columnDefinition: _Optional[_Union[TableColumnShared, _Mapping]] = ...) -> None: ...

class AlterColumnCommand(_message.Message):
    __slots__ = ("path", "tableName", "desiredDefiniton", "attributesToUpdate")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    DESIREDDEFINITON_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTESTOUPDATE_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    desiredDefiniton: TableColumnShared
    attributesToUpdate: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., desiredDefiniton: _Optional[_Union[TableColumnShared, _Mapping]] = ..., attributesToUpdate: _Optional[_Iterable[str]] = ...) -> None: ...

class DropColumnCommand(_message.Message):
    __slots__ = ("path", "tableName", "columnName")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNNAME_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    columnName: str
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., columnName: _Optional[str] = ...) -> None: ...

class AddPrimaryKeyCommand(_message.Message):
    __slots__ = ("path", "tableName", "primaryKeysNames")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    PRIMARYKEYSNAMES_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    primaryKeysNames: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., primaryKeysNames: _Optional[_Iterable[str]] = ...) -> None: ...

class DropPrimaryKeyCommand(_message.Message):
    __slots__ = ("path", "tableName")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ...) -> None: ...

class PreviewTableCommand(_message.Message):
    __slots__ = ("path", "tableName", "columns", "orderBy", "filters")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ORDERBY_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    orderBy: _containers.RepeatedCompositeFieldContainer[ImportExportShared.ExportOrderBy]
    filters: ImportExportShared.ExportFilters
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., orderBy: _Optional[_Iterable[_Union[ImportExportShared.ExportOrderBy, _Mapping]]] = ..., filters: _Optional[_Union[ImportExportShared.ExportFilters, _Mapping]] = ...) -> None: ...

class PreviewTableResponse(_message.Message):
    __slots__ = ("columns", "rows")
    class Row(_message.Message):
        __slots__ = ("columns",)
        class Column(_message.Message):
            __slots__ = ("columnName", "value", "isTruncated")
            COLUMNNAME_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            ISTRUNCATED_FIELD_NUMBER: _ClassVar[int]
            columnName: str
            value: _struct_pb2.Value
            isTruncated: bool
            def __init__(self, columnName: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ..., isTruncated: bool = ...) -> None: ...
        COLUMNS_FIELD_NUMBER: _ClassVar[int]
        columns: _containers.RepeatedCompositeFieldContainer[PreviewTableResponse.Row.Column]
        def __init__(self, columns: _Optional[_Iterable[_Union[PreviewTableResponse.Row.Column, _Mapping]]] = ...) -> None: ...
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    rows: _containers.RepeatedCompositeFieldContainer[PreviewTableResponse.Row]
    def __init__(self, columns: _Optional[_Iterable[str]] = ..., rows: _Optional[_Iterable[_Union[PreviewTableResponse.Row, _Mapping]]] = ...) -> None: ...

class ImportExportShared(_message.Message):
    __slots__ = ()
    class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STRING: _ClassVar[ImportExportShared.DataType]
        INTEGER: _ClassVar[ImportExportShared.DataType]
        DOUBLE: _ClassVar[ImportExportShared.DataType]
        BIGINT: _ClassVar[ImportExportShared.DataType]
        REAL: _ClassVar[ImportExportShared.DataType]
        DECIMAL: _ClassVar[ImportExportShared.DataType]
        TIMESTAMP: _ClassVar[ImportExportShared.DataType]
    STRING: ImportExportShared.DataType
    INTEGER: ImportExportShared.DataType
    DOUBLE: ImportExportShared.DataType
    BIGINT: ImportExportShared.DataType
    REAL: ImportExportShared.DataType
    DECIMAL: ImportExportShared.DataType
    TIMESTAMP: ImportExportShared.DataType
    class FileProvider(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        S3: _ClassVar[ImportExportShared.FileProvider]
        ABS: _ClassVar[ImportExportShared.FileProvider]
        GCS: _ClassVar[ImportExportShared.FileProvider]
        HTTP: _ClassVar[ImportExportShared.FileProvider]
    S3: ImportExportShared.FileProvider
    ABS: ImportExportShared.FileProvider
    GCS: ImportExportShared.FileProvider
    HTTP: ImportExportShared.FileProvider
    class FileFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CSV: _ClassVar[ImportExportShared.FileFormat]
    CSV: ImportExportShared.FileFormat
    class TableWhereFilter(_message.Message):
        __slots__ = ("columnsName", "operator", "values", "dataType")
        class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            eq: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
            ne: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
            gt: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
            ge: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
            lt: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
            le: _ClassVar[ImportExportShared.TableWhereFilter.Operator]
        eq: ImportExportShared.TableWhereFilter.Operator
        ne: ImportExportShared.TableWhereFilter.Operator
        gt: ImportExportShared.TableWhereFilter.Operator
        ge: ImportExportShared.TableWhereFilter.Operator
        lt: ImportExportShared.TableWhereFilter.Operator
        le: ImportExportShared.TableWhereFilter.Operator
        COLUMNSNAME_FIELD_NUMBER: _ClassVar[int]
        OPERATOR_FIELD_NUMBER: _ClassVar[int]
        VALUES_FIELD_NUMBER: _ClassVar[int]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        columnsName: str
        operator: ImportExportShared.TableWhereFilter.Operator
        values: _containers.RepeatedScalarFieldContainer[str]
        dataType: ImportExportShared.DataType
        def __init__(self, columnsName: _Optional[str] = ..., operator: _Optional[_Union[ImportExportShared.TableWhereFilter.Operator, str]] = ..., values: _Optional[_Iterable[str]] = ..., dataType: _Optional[_Union[ImportExportShared.DataType, str]] = ...) -> None: ...
    class Table(_message.Message):
        __slots__ = ("path", "tableName")
        PATH_FIELD_NUMBER: _ClassVar[int]
        TABLENAME_FIELD_NUMBER: _ClassVar[int]
        path: _containers.RepeatedScalarFieldContainer[str]
        tableName: str
        def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ...) -> None: ...
    class ImportOptions(_message.Message):
        __slots__ = ("timestampColumn", "convertEmptyValuesToNullOnColumns", "importType", "numberOfIgnoredLines", "dedupType", "dedupColumnsNames", "importStrategy", "createMode", "importAsNull")
        class ImportType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            FULL: _ClassVar[ImportExportShared.ImportOptions.ImportType]
            INCREMENTAL: _ClassVar[ImportExportShared.ImportOptions.ImportType]
            VIEW: _ClassVar[ImportExportShared.ImportOptions.ImportType]
            CLONE: _ClassVar[ImportExportShared.ImportOptions.ImportType]
        FULL: ImportExportShared.ImportOptions.ImportType
        INCREMENTAL: ImportExportShared.ImportOptions.ImportType
        VIEW: ImportExportShared.ImportOptions.ImportType
        CLONE: ImportExportShared.ImportOptions.ImportType
        class DedupType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            UPDATE_DUPLICATES: _ClassVar[ImportExportShared.ImportOptions.DedupType]
            INSERT_DUPLICATES: _ClassVar[ImportExportShared.ImportOptions.DedupType]
            FAIL_ON_DUPLICATES: _ClassVar[ImportExportShared.ImportOptions.DedupType]
        UPDATE_DUPLICATES: ImportExportShared.ImportOptions.DedupType
        INSERT_DUPLICATES: ImportExportShared.ImportOptions.DedupType
        FAIL_ON_DUPLICATES: ImportExportShared.ImportOptions.DedupType
        class ImportStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            STRING_TABLE: _ClassVar[ImportExportShared.ImportOptions.ImportStrategy]
            USER_DEFINED_TABLE: _ClassVar[ImportExportShared.ImportOptions.ImportStrategy]
        STRING_TABLE: ImportExportShared.ImportOptions.ImportStrategy
        USER_DEFINED_TABLE: ImportExportShared.ImportOptions.ImportStrategy
        class CreateMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            CREATE: _ClassVar[ImportExportShared.ImportOptions.CreateMode]
            REPLACE: _ClassVar[ImportExportShared.ImportOptions.CreateMode]
        CREATE: ImportExportShared.ImportOptions.CreateMode
        REPLACE: ImportExportShared.ImportOptions.CreateMode
        TIMESTAMPCOLUMN_FIELD_NUMBER: _ClassVar[int]
        CONVERTEMPTYVALUESTONULLONCOLUMNS_FIELD_NUMBER: _ClassVar[int]
        IMPORTTYPE_FIELD_NUMBER: _ClassVar[int]
        NUMBEROFIGNOREDLINES_FIELD_NUMBER: _ClassVar[int]
        DEDUPTYPE_FIELD_NUMBER: _ClassVar[int]
        DEDUPCOLUMNSNAMES_FIELD_NUMBER: _ClassVar[int]
        IMPORTSTRATEGY_FIELD_NUMBER: _ClassVar[int]
        CREATEMODE_FIELD_NUMBER: _ClassVar[int]
        IMPORTASNULL_FIELD_NUMBER: _ClassVar[int]
        timestampColumn: str
        convertEmptyValuesToNullOnColumns: _containers.RepeatedScalarFieldContainer[str]
        importType: ImportExportShared.ImportOptions.ImportType
        numberOfIgnoredLines: int
        dedupType: ImportExportShared.ImportOptions.DedupType
        dedupColumnsNames: _containers.RepeatedScalarFieldContainer[str]
        importStrategy: ImportExportShared.ImportOptions.ImportStrategy
        createMode: ImportExportShared.ImportOptions.CreateMode
        importAsNull: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, timestampColumn: _Optional[str] = ..., convertEmptyValuesToNullOnColumns: _Optional[_Iterable[str]] = ..., importType: _Optional[_Union[ImportExportShared.ImportOptions.ImportType, str]] = ..., numberOfIgnoredLines: _Optional[int] = ..., dedupType: _Optional[_Union[ImportExportShared.ImportOptions.DedupType, str]] = ..., dedupColumnsNames: _Optional[_Iterable[str]] = ..., importStrategy: _Optional[_Union[ImportExportShared.ImportOptions.ImportStrategy, str]] = ..., createMode: _Optional[_Union[ImportExportShared.ImportOptions.CreateMode, str]] = ..., importAsNull: _Optional[_Iterable[str]] = ...) -> None: ...
    class ExportOptions(_message.Message):
        __slots__ = ("isCompressed", "columnsToExport", "orderBy", "filters")
        ISCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
        COLUMNSTOEXPORT_FIELD_NUMBER: _ClassVar[int]
        ORDERBY_FIELD_NUMBER: _ClassVar[int]
        FILTERS_FIELD_NUMBER: _ClassVar[int]
        isCompressed: bool
        columnsToExport: _containers.RepeatedScalarFieldContainer[str]
        orderBy: _containers.RepeatedCompositeFieldContainer[ImportExportShared.ExportOrderBy]
        filters: ImportExportShared.ExportFilters
        def __init__(self, isCompressed: bool = ..., columnsToExport: _Optional[_Iterable[str]] = ..., orderBy: _Optional[_Iterable[_Union[ImportExportShared.ExportOrderBy, _Mapping]]] = ..., filters: _Optional[_Union[ImportExportShared.ExportFilters, _Mapping]] = ...) -> None: ...
    class ExportFilters(_message.Message):
        __slots__ = ("limit", "changeSince", "changeUntil", "fulltextSearch", "whereFilters")
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        CHANGESINCE_FIELD_NUMBER: _ClassVar[int]
        CHANGEUNTIL_FIELD_NUMBER: _ClassVar[int]
        FULLTEXTSEARCH_FIELD_NUMBER: _ClassVar[int]
        WHEREFILTERS_FIELD_NUMBER: _ClassVar[int]
        limit: int
        changeSince: str
        changeUntil: str
        fulltextSearch: str
        whereFilters: _containers.RepeatedCompositeFieldContainer[ImportExportShared.TableWhereFilter]
        def __init__(self, limit: _Optional[int] = ..., changeSince: _Optional[str] = ..., changeUntil: _Optional[str] = ..., fulltextSearch: _Optional[str] = ..., whereFilters: _Optional[_Iterable[_Union[ImportExportShared.TableWhereFilter, _Mapping]]] = ...) -> None: ...
    class ExportOrderBy(_message.Message):
        __slots__ = ("columnName", "order", "dataType")
        class Order(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            ASC: _ClassVar[ImportExportShared.ExportOrderBy.Order]
            DESC: _ClassVar[ImportExportShared.ExportOrderBy.Order]
        ASC: ImportExportShared.ExportOrderBy.Order
        DESC: ImportExportShared.ExportOrderBy.Order
        COLUMNNAME_FIELD_NUMBER: _ClassVar[int]
        ORDER_FIELD_NUMBER: _ClassVar[int]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        columnName: str
        order: ImportExportShared.ExportOrderBy.Order
        dataType: ImportExportShared.DataType
        def __init__(self, columnName: _Optional[str] = ..., order: _Optional[_Union[ImportExportShared.ExportOrderBy.Order, str]] = ..., dataType: _Optional[_Union[ImportExportShared.DataType, str]] = ...) -> None: ...
    class S3Credentials(_message.Message):
        __slots__ = ("key", "secret", "region", "token")
        KEY_FIELD_NUMBER: _ClassVar[int]
        SECRET_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        TOKEN_FIELD_NUMBER: _ClassVar[int]
        key: str
        secret: str
        region: str
        token: str
        def __init__(self, key: _Optional[str] = ..., secret: _Optional[str] = ..., region: _Optional[str] = ..., token: _Optional[str] = ...) -> None: ...
    class ABSCredentials(_message.Message):
        __slots__ = ("accountName", "sasToken", "accountKey")
        ACCOUNTNAME_FIELD_NUMBER: _ClassVar[int]
        SASTOKEN_FIELD_NUMBER: _ClassVar[int]
        ACCOUNTKEY_FIELD_NUMBER: _ClassVar[int]
        accountName: str
        sasToken: str
        accountKey: str
        def __init__(self, accountName: _Optional[str] = ..., sasToken: _Optional[str] = ..., accountKey: _Optional[str] = ...) -> None: ...
    class GCSCredentials(_message.Message):
        __slots__ = ("key", "secret")
        KEY_FIELD_NUMBER: _ClassVar[int]
        SECRET_FIELD_NUMBER: _ClassVar[int]
        key: str
        secret: str
        def __init__(self, key: _Optional[str] = ..., secret: _Optional[str] = ...) -> None: ...
    class FilePath(_message.Message):
        __slots__ = ("root", "path", "fileName")
        ROOT_FIELD_NUMBER: _ClassVar[int]
        PATH_FIELD_NUMBER: _ClassVar[int]
        FILENAME_FIELD_NUMBER: _ClassVar[int]
        root: str
        path: str
        fileName: str
        def __init__(self, root: _Optional[str] = ..., path: _Optional[str] = ..., fileName: _Optional[str] = ...) -> None: ...
    def __init__(self) -> None: ...

class TableImportFromFileCommand(_message.Message):
    __slots__ = ("fileProvider", "fileFormat", "formatTypeOptions", "filePath", "fileCredentials", "destination", "importOptions", "meta")
    class CsvTypeOptions(_message.Message):
        __slots__ = ("columnsNames", "delimiter", "enclosure", "escapedBy", "sourceType", "compression")
        class SourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            SINGLE_FILE: _ClassVar[TableImportFromFileCommand.CsvTypeOptions.SourceType]
            SLICED_FILE: _ClassVar[TableImportFromFileCommand.CsvTypeOptions.SourceType]
            DIRECTORY: _ClassVar[TableImportFromFileCommand.CsvTypeOptions.SourceType]
        SINGLE_FILE: TableImportFromFileCommand.CsvTypeOptions.SourceType
        SLICED_FILE: TableImportFromFileCommand.CsvTypeOptions.SourceType
        DIRECTORY: TableImportFromFileCommand.CsvTypeOptions.SourceType
        class Compression(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            NONE: _ClassVar[TableImportFromFileCommand.CsvTypeOptions.Compression]
            GZIP: _ClassVar[TableImportFromFileCommand.CsvTypeOptions.Compression]
        NONE: TableImportFromFileCommand.CsvTypeOptions.Compression
        GZIP: TableImportFromFileCommand.CsvTypeOptions.Compression
        COLUMNSNAMES_FIELD_NUMBER: _ClassVar[int]
        DELIMITER_FIELD_NUMBER: _ClassVar[int]
        ENCLOSURE_FIELD_NUMBER: _ClassVar[int]
        ESCAPEDBY_FIELD_NUMBER: _ClassVar[int]
        SOURCETYPE_FIELD_NUMBER: _ClassVar[int]
        COMPRESSION_FIELD_NUMBER: _ClassVar[int]
        columnsNames: _containers.RepeatedScalarFieldContainer[str]
        delimiter: str
        enclosure: str
        escapedBy: str
        sourceType: TableImportFromFileCommand.CsvTypeOptions.SourceType
        compression: TableImportFromFileCommand.CsvTypeOptions.Compression
        def __init__(self, columnsNames: _Optional[_Iterable[str]] = ..., delimiter: _Optional[str] = ..., enclosure: _Optional[str] = ..., escapedBy: _Optional[str] = ..., sourceType: _Optional[_Union[TableImportFromFileCommand.CsvTypeOptions.SourceType, str]] = ..., compression: _Optional[_Union[TableImportFromFileCommand.CsvTypeOptions.Compression, str]] = ...) -> None: ...
    class TeradataTableImportMeta(_message.Message):
        __slots__ = ("importAdapter",)
        class ImportAdapter(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            TPT: _ClassVar[TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter]
        TPT: TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter
        IMPORTADAPTER_FIELD_NUMBER: _ClassVar[int]
        importAdapter: TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter
        def __init__(self, importAdapter: _Optional[_Union[TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter, str]] = ...) -> None: ...
    FILEPROVIDER_FIELD_NUMBER: _ClassVar[int]
    FILEFORMAT_FIELD_NUMBER: _ClassVar[int]
    FORMATTYPEOPTIONS_FIELD_NUMBER: _ClassVar[int]
    FILEPATH_FIELD_NUMBER: _ClassVar[int]
    FILECREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    IMPORTOPTIONS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    fileProvider: ImportExportShared.FileProvider
    fileFormat: ImportExportShared.FileFormat
    formatTypeOptions: _any_pb2.Any
    filePath: ImportExportShared.FilePath
    fileCredentials: _any_pb2.Any
    destination: ImportExportShared.Table
    importOptions: ImportExportShared.ImportOptions
    meta: _any_pb2.Any
    def __init__(self, fileProvider: _Optional[_Union[ImportExportShared.FileProvider, str]] = ..., fileFormat: _Optional[_Union[ImportExportShared.FileFormat, str]] = ..., formatTypeOptions: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., filePath: _Optional[_Union[ImportExportShared.FilePath, _Mapping]] = ..., fileCredentials: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., destination: _Optional[_Union[ImportExportShared.Table, _Mapping]] = ..., importOptions: _Optional[_Union[ImportExportShared.ImportOptions, _Mapping]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class TableImportResponse(_message.Message):
    __slots__ = ("importedRowsCount", "tableRowsCount", "tableSizeBytes", "timers", "importedColumns", "meta")
    class Timer(_message.Message):
        __slots__ = ("name", "duration")
        NAME_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        name: str
        duration: str
        def __init__(self, name: _Optional[str] = ..., duration: _Optional[str] = ...) -> None: ...
    class TeradataTableImportMeta(_message.Message):
        __slots__ = ("importLog", "errorTable1records", "errorTable2records")
        IMPORTLOG_FIELD_NUMBER: _ClassVar[int]
        ERRORTABLE1RECORDS_FIELD_NUMBER: _ClassVar[int]
        ERRORTABLE2RECORDS_FIELD_NUMBER: _ClassVar[int]
        importLog: str
        errorTable1records: str
        errorTable2records: str
        def __init__(self, importLog: _Optional[str] = ..., errorTable1records: _Optional[str] = ..., errorTable2records: _Optional[str] = ...) -> None: ...
    IMPORTEDROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    TABLEROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    TABLESIZEBYTES_FIELD_NUMBER: _ClassVar[int]
    TIMERS_FIELD_NUMBER: _ClassVar[int]
    IMPORTEDCOLUMNS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    importedRowsCount: int
    tableRowsCount: int
    tableSizeBytes: int
    timers: _containers.RepeatedCompositeFieldContainer[TableImportResponse.Timer]
    importedColumns: _containers.RepeatedScalarFieldContainer[str]
    meta: _any_pb2.Any
    def __init__(self, importedRowsCount: _Optional[int] = ..., tableRowsCount: _Optional[int] = ..., tableSizeBytes: _Optional[int] = ..., timers: _Optional[_Iterable[_Union[TableImportResponse.Timer, _Mapping]]] = ..., importedColumns: _Optional[_Iterable[str]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class TableImportFromTableCommand(_message.Message):
    __slots__ = ("source", "destination", "importOptions")
    class SourceTableMapping(_message.Message):
        __slots__ = ("path", "tableName", "seconds", "whereFilters", "limit", "columnMappings")
        class ColumnMapping(_message.Message):
            __slots__ = ("sourceColumnName", "destinationColumnName")
            SOURCECOLUMNNAME_FIELD_NUMBER: _ClassVar[int]
            DESTINATIONCOLUMNNAME_FIELD_NUMBER: _ClassVar[int]
            sourceColumnName: str
            destinationColumnName: str
            def __init__(self, sourceColumnName: _Optional[str] = ..., destinationColumnName: _Optional[str] = ...) -> None: ...
        PATH_FIELD_NUMBER: _ClassVar[int]
        TABLENAME_FIELD_NUMBER: _ClassVar[int]
        SECONDS_FIELD_NUMBER: _ClassVar[int]
        WHEREFILTERS_FIELD_NUMBER: _ClassVar[int]
        LIMIT_FIELD_NUMBER: _ClassVar[int]
        COLUMNMAPPINGS_FIELD_NUMBER: _ClassVar[int]
        path: _containers.RepeatedScalarFieldContainer[str]
        tableName: str
        seconds: int
        whereFilters: _containers.RepeatedCompositeFieldContainer[ImportExportShared.TableWhereFilter]
        limit: int
        columnMappings: _containers.RepeatedCompositeFieldContainer[TableImportFromTableCommand.SourceTableMapping.ColumnMapping]
        def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., seconds: _Optional[int] = ..., whereFilters: _Optional[_Iterable[_Union[ImportExportShared.TableWhereFilter, _Mapping]]] = ..., limit: _Optional[int] = ..., columnMappings: _Optional[_Iterable[_Union[TableImportFromTableCommand.SourceTableMapping.ColumnMapping, _Mapping]]] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    IMPORTOPTIONS_FIELD_NUMBER: _ClassVar[int]
    source: TableImportFromTableCommand.SourceTableMapping
    destination: ImportExportShared.Table
    importOptions: ImportExportShared.ImportOptions
    def __init__(self, source: _Optional[_Union[TableImportFromTableCommand.SourceTableMapping, _Mapping]] = ..., destination: _Optional[_Union[ImportExportShared.Table, _Mapping]] = ..., importOptions: _Optional[_Union[ImportExportShared.ImportOptions, _Mapping]] = ...) -> None: ...

class TableExportToFileCommand(_message.Message):
    __slots__ = ("source", "fileProvider", "fileFormat", "filePath", "fileCredentials", "exportOptions", "meta")
    class TeradataTableExportMeta(_message.Message):
        __slots__ = ("exportAdapter",)
        class ExportAdapter(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            TPT: _ClassVar[TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter]
        TPT: TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter
        EXPORTADAPTER_FIELD_NUMBER: _ClassVar[int]
        exportAdapter: TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter
        def __init__(self, exportAdapter: _Optional[_Union[TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter, str]] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    FILEPROVIDER_FIELD_NUMBER: _ClassVar[int]
    FILEFORMAT_FIELD_NUMBER: _ClassVar[int]
    FILEPATH_FIELD_NUMBER: _ClassVar[int]
    FILECREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    EXPORTOPTIONS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    source: ImportExportShared.Table
    fileProvider: ImportExportShared.FileProvider
    fileFormat: ImportExportShared.FileFormat
    filePath: ImportExportShared.FilePath
    fileCredentials: _any_pb2.Any
    exportOptions: ImportExportShared.ExportOptions
    meta: _any_pb2.Any
    def __init__(self, source: _Optional[_Union[ImportExportShared.Table, _Mapping]] = ..., fileProvider: _Optional[_Union[ImportExportShared.FileProvider, str]] = ..., fileFormat: _Optional[_Union[ImportExportShared.FileFormat, str]] = ..., filePath: _Optional[_Union[ImportExportShared.FilePath, _Mapping]] = ..., fileCredentials: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., exportOptions: _Optional[_Union[ImportExportShared.ExportOptions, _Mapping]] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class TableExportToFileResponse(_message.Message):
    __slots__ = ("tableInfo",)
    TABLEINFO_FIELD_NUMBER: _ClassVar[int]
    tableInfo: _info_pb2.TableInfo
    def __init__(self, tableInfo: _Optional[_Union[_info_pb2.TableInfo, _Mapping]] = ...) -> None: ...

class DeleteTableRowsCommand(_message.Message):
    __slots__ = ("path", "tableName", "changeSince", "changeUntil", "whereFilters", "whereRefTableFilters")
    class WhereRefTableFilter(_message.Message):
        __slots__ = ("column", "operator", "refPath", "refTable", "refColumn")
        class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            IN: _ClassVar[DeleteTableRowsCommand.WhereRefTableFilter.Operator]
            NOT_IN: _ClassVar[DeleteTableRowsCommand.WhereRefTableFilter.Operator]
        IN: DeleteTableRowsCommand.WhereRefTableFilter.Operator
        NOT_IN: DeleteTableRowsCommand.WhereRefTableFilter.Operator
        COLUMN_FIELD_NUMBER: _ClassVar[int]
        OPERATOR_FIELD_NUMBER: _ClassVar[int]
        REFPATH_FIELD_NUMBER: _ClassVar[int]
        REFTABLE_FIELD_NUMBER: _ClassVar[int]
        REFCOLUMN_FIELD_NUMBER: _ClassVar[int]
        column: str
        operator: DeleteTableRowsCommand.WhereRefTableFilter.Operator
        refPath: _containers.RepeatedScalarFieldContainer[str]
        refTable: str
        refColumn: str
        def __init__(self, column: _Optional[str] = ..., operator: _Optional[_Union[DeleteTableRowsCommand.WhereRefTableFilter.Operator, str]] = ..., refPath: _Optional[_Iterable[str]] = ..., refTable: _Optional[str] = ..., refColumn: _Optional[str] = ...) -> None: ...
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    CHANGESINCE_FIELD_NUMBER: _ClassVar[int]
    CHANGEUNTIL_FIELD_NUMBER: _ClassVar[int]
    WHEREFILTERS_FIELD_NUMBER: _ClassVar[int]
    WHEREREFTABLEFILTERS_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    changeSince: str
    changeUntil: str
    whereFilters: _containers.RepeatedCompositeFieldContainer[ImportExportShared.TableWhereFilter]
    whereRefTableFilters: _containers.RepeatedCompositeFieldContainer[DeleteTableRowsCommand.WhereRefTableFilter]
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., changeSince: _Optional[str] = ..., changeUntil: _Optional[str] = ..., whereFilters: _Optional[_Iterable[_Union[ImportExportShared.TableWhereFilter, _Mapping]]] = ..., whereRefTableFilters: _Optional[_Iterable[_Union[DeleteTableRowsCommand.WhereRefTableFilter, _Mapping]]] = ...) -> None: ...

class DeleteTableRowsResponse(_message.Message):
    __slots__ = ("deletedRowsCount", "tableRowsCount", "tableSizeBytes")
    DELETEDROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    TABLEROWSCOUNT_FIELD_NUMBER: _ClassVar[int]
    TABLESIZEBYTES_FIELD_NUMBER: _ClassVar[int]
    deletedRowsCount: int
    tableRowsCount: int
    tableSizeBytes: int
    def __init__(self, deletedRowsCount: _Optional[int] = ..., tableRowsCount: _Optional[int] = ..., tableSizeBytes: _Optional[int] = ...) -> None: ...

class CreateTableFromTimeTravelCommand(_message.Message):
    __slots__ = ("source", "destination", "timestamp")
    class SourceTableMapping(_message.Message):
        __slots__ = ("path", "tableName")
        PATH_FIELD_NUMBER: _ClassVar[int]
        TABLENAME_FIELD_NUMBER: _ClassVar[int]
        path: _containers.RepeatedScalarFieldContainer[str]
        tableName: str
        def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    source: CreateTableFromTimeTravelCommand.SourceTableMapping
    destination: ImportExportShared.Table
    timestamp: int
    def __init__(self, source: _Optional[_Union[CreateTableFromTimeTravelCommand.SourceTableMapping, _Mapping]] = ..., destination: _Optional[_Union[ImportExportShared.Table, _Mapping]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class CreateProfileTableCommand(_message.Message):
    __slots__ = ("path", "tableName")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ...) -> None: ...

class CreateProfileTableResponse(_message.Message):
    __slots__ = ("path", "tableName", "profile", "columns")
    class Column(_message.Message):
        __slots__ = ("name", "profile")
        NAME_FIELD_NUMBER: _ClassVar[int]
        PROFILE_FIELD_NUMBER: _ClassVar[int]
        name: str
        profile: str
        def __init__(self, name: _Optional[str] = ..., profile: _Optional[str] = ...) -> None: ...
    PATH_FIELD_NUMBER: _ClassVar[int]
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    tableName: str
    profile: str
    columns: _containers.RepeatedCompositeFieldContainer[CreateProfileTableResponse.Column]
    def __init__(self, path: _Optional[_Iterable[str]] = ..., tableName: _Optional[str] = ..., profile: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[CreateProfileTableResponse.Column, _Mapping]]] = ...) -> None: ...
