from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateBucketCommand(_message.Message):
    __slots__ = ("stackPrefix", "projectId", "bucketId", "branchId", "projectRoleName", "projectReadOnlyRoleName", "devBranchReadOnlyRoleName", "isBranchDefault", "meta")
    class CreateBucketTeradataMeta(_message.Message):
        __slots__ = ("permSpace", "spoolSpace")
        PERMSPACE_FIELD_NUMBER: _ClassVar[int]
        SPOOLSPACE_FIELD_NUMBER: _ClassVar[int]
        permSpace: str
        spoolSpace: str
        def __init__(self, permSpace: _Optional[str] = ..., spoolSpace: _Optional[str] = ...) -> None: ...
    class CreateBucketBigqueryMeta(_message.Message):
        __slots__ = ("region",)
        REGION_FIELD_NUMBER: _ClassVar[int]
        region: str
        def __init__(self, region: _Optional[str] = ...) -> None: ...
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    PROJECTID_FIELD_NUMBER: _ClassVar[int]
    BUCKETID_FIELD_NUMBER: _ClassVar[int]
    BRANCHID_FIELD_NUMBER: _ClassVar[int]
    PROJECTROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    DEVBRANCHREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    ISBRANCHDEFAULT_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    projectId: str
    bucketId: str
    branchId: str
    projectRoleName: str
    projectReadOnlyRoleName: str
    devBranchReadOnlyRoleName: str
    isBranchDefault: bool
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., projectId: _Optional[str] = ..., bucketId: _Optional[str] = ..., branchId: _Optional[str] = ..., projectRoleName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., devBranchReadOnlyRoleName: _Optional[str] = ..., isBranchDefault: bool = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class CreateBucketResponse(_message.Message):
    __slots__ = ("path", "createBucketObjectName")
    PATH_FIELD_NUMBER: _ClassVar[int]
    CREATEBUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    path: _containers.RepeatedScalarFieldContainer[str]
    createBucketObjectName: str
    def __init__(self, path: _Optional[_Iterable[str]] = ..., createBucketObjectName: _Optional[str] = ...) -> None: ...

class DropBucketCommand(_message.Message):
    __slots__ = ("bucketObjectName", "ignoreErrors", "projectReadOnlyRoleName", "meta", "isCascade")
    BUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    IGNOREERRORS_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    ISCASCADE_FIELD_NUMBER: _ClassVar[int]
    bucketObjectName: str
    ignoreErrors: bool
    projectReadOnlyRoleName: str
    meta: _any_pb2.Any
    isCascade: bool
    def __init__(self, bucketObjectName: _Optional[str] = ..., ignoreErrors: bool = ..., projectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., isCascade: bool = ...) -> None: ...

class LinkBucketCommand(_message.Message):
    __slots__ = ("stackPrefix", "targetProjectId", "targetBucketId", "sourceShareRoleName", "targetProjectReadOnlyRoleName", "meta")
    class LinkBucketBigqueryMeta(_message.Message):
        __slots__ = ("region",)
        REGION_FIELD_NUMBER: _ClassVar[int]
        region: str
        def __init__(self, region: _Optional[str] = ...) -> None: ...
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    TARGETPROJECTID_FIELD_NUMBER: _ClassVar[int]
    TARGETBUCKETID_FIELD_NUMBER: _ClassVar[int]
    SOURCESHAREROLENAME_FIELD_NUMBER: _ClassVar[int]
    TARGETPROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    targetProjectId: str
    targetBucketId: str
    sourceShareRoleName: str
    targetProjectReadOnlyRoleName: str
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., targetProjectId: _Optional[str] = ..., targetBucketId: _Optional[str] = ..., sourceShareRoleName: _Optional[str] = ..., targetProjectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class LinkedBucketResponse(_message.Message):
    __slots__ = ("linkedBucketObjectName",)
    LINKEDBUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    linkedBucketObjectName: str
    def __init__(self, linkedBucketObjectName: _Optional[str] = ...) -> None: ...

class ShareBucketCommand(_message.Message):
    __slots__ = ("stackPrefix", "sourceBucketObjectName", "sourceProjectReadOnlyRoleName", "sourceProjectId", "sourceBucketId", "meta")
    class ShareBucketSnowflakeCommandMeta(_message.Message):
        __slots__ = ("databaseName",)
        DATABASENAME_FIELD_NUMBER: _ClassVar[int]
        databaseName: str
        def __init__(self, databaseName: _Optional[str] = ...) -> None: ...
    class ShareBucketBigqueryCommandMeta(_message.Message):
        __slots__ = ("region",)
        REGION_FIELD_NUMBER: _ClassVar[int]
        region: str
        def __init__(self, region: _Optional[str] = ...) -> None: ...
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    SOURCEBUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    SOURCEPROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    SOURCEPROJECTID_FIELD_NUMBER: _ClassVar[int]
    SOURCEBUCKETID_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    stackPrefix: str
    sourceBucketObjectName: str
    sourceProjectReadOnlyRoleName: str
    sourceProjectId: str
    sourceBucketId: str
    meta: _any_pb2.Any
    def __init__(self, stackPrefix: _Optional[str] = ..., sourceBucketObjectName: _Optional[str] = ..., sourceProjectReadOnlyRoleName: _Optional[str] = ..., sourceProjectId: _Optional[str] = ..., sourceBucketId: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class ShareBucketResponse(_message.Message):
    __slots__ = ("bucketShareRoleName",)
    BUCKETSHAREROLENAME_FIELD_NUMBER: _ClassVar[int]
    bucketShareRoleName: str
    def __init__(self, bucketShareRoleName: _Optional[str] = ...) -> None: ...

class UnlinkBucketCommand(_message.Message):
    __slots__ = ("bucketObjectName", "sourceShareRoleName", "projectReadOnlyRoleName", "meta")
    BUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    SOURCESHAREROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    bucketObjectName: str
    sourceShareRoleName: str
    projectReadOnlyRoleName: str
    meta: _any_pb2.Any
    def __init__(self, bucketObjectName: _Optional[str] = ..., sourceShareRoleName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class UnshareBucketCommand(_message.Message):
    __slots__ = ("bucketObjectName", "bucketShareRoleName", "projectReadOnlyRoleName", "meta")
    BUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    BUCKETSHAREROLENAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    bucketObjectName: str
    bucketShareRoleName: str
    projectReadOnlyRoleName: str
    meta: _any_pb2.Any
    def __init__(self, bucketObjectName: _Optional[str] = ..., bucketShareRoleName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class GrantBucketAccessToReadOnlyRoleCommand(_message.Message):
    __slots__ = ("projectReadOnlyRoleName", "meta", "branchId", "stackPrefix", "destinationObjectName", "path")
    class GrantBucketAccessToReadOnlyRoleBigqueryMeta(_message.Message):
        __slots__ = ("region",)
        REGION_FIELD_NUMBER: _ClassVar[int]
        region: str
        def __init__(self, region: _Optional[str] = ...) -> None: ...
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    BRANCHID_FIELD_NUMBER: _ClassVar[int]
    STACKPREFIX_FIELD_NUMBER: _ClassVar[int]
    DESTINATIONOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    projectReadOnlyRoleName: str
    meta: _any_pb2.Any
    branchId: str
    stackPrefix: str
    destinationObjectName: str
    path: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, projectReadOnlyRoleName: _Optional[str] = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., branchId: _Optional[str] = ..., stackPrefix: _Optional[str] = ..., destinationObjectName: _Optional[str] = ..., path: _Optional[_Iterable[str]] = ...) -> None: ...

class GrantBucketAccessToReadOnlyRoleResponse(_message.Message):
    __slots__ = ("createBucketObjectName",)
    CREATEBUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    createBucketObjectName: str
    def __init__(self, createBucketObjectName: _Optional[str] = ...) -> None: ...

class RevokeBucketAccessFromReadOnlyRoleCommand(_message.Message):
    __slots__ = ("bucketObjectName", "projectReadOnlyRoleName", "ignoreErrors", "meta")
    BUCKETOBJECTNAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTREADONLYROLENAME_FIELD_NUMBER: _ClassVar[int]
    IGNOREERRORS_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    bucketObjectName: str
    projectReadOnlyRoleName: str
    ignoreErrors: bool
    meta: _any_pb2.Any
    def __init__(self, bucketObjectName: _Optional[str] = ..., projectReadOnlyRoleName: _Optional[str] = ..., ignoreErrors: bool = ..., meta: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
