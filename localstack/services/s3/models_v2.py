import hashlib
import logging
from collections import defaultdict
from datetime import datetime
from io import RawIOBase
from secrets import token_urlsafe
from typing import IO, Literal, Optional, Union

from werkzeug.datastructures.headers import Headers

from localstack import config
from localstack.aws.api.s3 import (  # BucketCannedACL,; ServerSideEncryptionRules,; Body,
    AccountId,
    AnalyticsConfiguration,
    AnalyticsId,
    BucketAccelerateStatus,
    BucketName,
    BucketRegion,
    ChecksumAlgorithm,
    CompletedPartList,
    CORSConfiguration,
    ETag,
    IntelligentTieringConfiguration,
    IntelligentTieringId,
    InvalidArgument,
    LifecycleRules,
    LoggingEnabled,
    Metadata,
    MethodNotAllowed,
    MultipartUploadId,
    NoSuchKey,
    NoSuchVersion,
    NotificationConfiguration,
    ObjectKey,
    ObjectLockConfiguration,
    ObjectLockLegalHoldStatus,
    ObjectLockMode,
    ObjectOwnership,
    ObjectStorageClass,
    ObjectVersionId,
    Owner,
    PartNumber,
    Payer,
    Policy,
    PublicAccessBlockConfiguration,
    ReplicationConfiguration,
    ServerSideEncryption,
    ServerSideEncryptionRule,
    Size,
    SSEKMSKeyId,
    StorageClass,
    WebsiteConfiguration,
    WebsiteRedirectLocation,
)
from localstack.services.s3.constants import S3_CHUNK_SIZE, S3_UPLOAD_PART_MIN_SIZE
from localstack.services.s3.storage import LockedSpooledTemporaryFile
from localstack.services.s3.utils import ParsedRange, get_owner_for_account_id
from localstack.services.stores import (
    AccountRegionBundle,
    BaseStore,
    CrossAccountAttribute,
    CrossRegionAttribute,
)

# TODO: beware of timestamp data, we need the snapshot to be more precise for S3, with the different types
# moto had a lot of issue with it? not sure about our parser/serializer

# for persistence, append the version id to the key name using a special symbol?? like __version_id__={version_id}

# TODO: we need to make the SpooledTemporaryFile configurable for persistence?
LOG = logging.getLogger(__name__)


# TODO move to utils?
def iso_8601_datetime_without_milliseconds_s3(
    value: datetime,
) -> Optional[str]:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z") if value else None


RFC1123 = "%a, %d %b %Y %H:%M:%S GMT"


def rfc_1123_datetime(src: datetime) -> str:
    return src.strftime(RFC1123)


def str_to_rfc_1123_datetime(value: str) -> datetime:
    return datetime.strptime(value, RFC1123)


# TODO: we will need a versioned key store as well, let's check what we can get better
class S3Bucket:
    name: BucketName
    bucket_account_id: AccountId
    bucket_region: BucketRegion
    creation_date: datetime
    multiparts: dict[MultipartUploadId, "S3Multipart"]  # TODO: is there a key thing here?
    objects: Union["KeyStore", "VersionedKeyStore"]
    versioning_status: Literal[None, "Enabled", "Disabled"]
    lifecycle_rules: LifecycleRules
    policy: Optional[Policy]
    website_configuration: WebsiteConfiguration
    acl: str  # TODO: change this
    cors_rules: CORSConfiguration
    logging: LoggingEnabled
    notification_configuration: NotificationConfiguration
    payer: Payer
    encryption_rule: Optional[
        ServerSideEncryptionRule
    ]  # TODO validate if there can be more than one rule
    public_access_block: PublicAccessBlockConfiguration
    accelerate_status: BucketAccelerateStatus
    object_ownership: ObjectOwnership
    object_lock_configuration: Optional[ObjectLockConfiguration]
    object_lock_enabled: bool
    intelligent_tiering_configuration: dict[IntelligentTieringId, IntelligentTieringConfiguration]
    analytics_configuration: dict[AnalyticsId, AnalyticsConfiguration]
    replication: ReplicationConfiguration
    owner: Owner

    # set all buckets parameters here
    # first one in moto, then one in our provider added (cors, lifecycle and such)
    def __init__(
        self,
        name: BucketName,
        account_id: AccountId,
        bucket_region: BucketRegion,
        acl=None,  # TODO: validate ACL first, create utils for validating and consolidating
        object_ownership: ObjectOwnership = None,
        object_lock_enabled_for_bucket: bool = None,
    ):
        self.name = name
        self.bucket_account_id = account_id
        self.bucket_region = bucket_region
        self.objects = KeyStore()
        # self.acl
        self.object_ownership = object_ownership
        self.object_lock_enabled = object_lock_enabled_for_bucket
        self.encryption_rule = None  # TODO
        self.creation_date = datetime.now()
        self.multiparts = {}
        # we set the versioning status to None instead of False to be able to differentiate between a bucket which
        # was enabled at some point and one fresh bucket
        self.versioning_status = None

        # see https://docs.aws.amazon.com/AmazonS3/latest/API/API_Owner.html
        self.owner = get_owner_for_account_id(account_id)

    def get_object(
        self,
        key: ObjectKey,
        version_id: ObjectVersionId = None,
        http_method: Literal["GET", "PUT", "HEAD"] = "GET",  # TODO: better?
    ) -> "S3Object":
        """
        :param key: the Object Key
        :param version_id: optional, the versionId of the object
        :param http_method: the HTTP method of the original call. This is necessary for the exception if the bucket is
        versioned or suspended
        see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
        :return:
        :raises NoSuchKey if the object key does not exist at all, or if the object is a DeleteMarker
        :raises MethodNotAllowed if the object is a DeleteMarker and the operation is not allowed against it
        """

        if self.versioning_status is None:
            if version_id:
                raise InvalidArgument(
                    "Invalid version id specified",
                    ArgumentName="versionId",
                    ArgumentValue=version_id,
                )

            s3_object = self.objects.get(key)

            if not s3_object:
                raise NoSuchKey("The specified key does not exist.", Key=key)

        else:
            self.objects: VersionedKeyStore
            if version_id:
                s3_object_version = self.objects.get(key, version_id)
                if not s3_object_version:
                    raise NoSuchVersion(
                        "The specified version does not exist.",
                        Key=key,
                        VersionId=version_id,
                    )
                elif isinstance(s3_object_version, S3DeleteMarker):
                    raise MethodNotAllowed(
                        "The specified method is not allowed against this resource.",
                        Method=http_method,
                        ResourceType="DeleteMarker",
                        DeleteMarker=True,
                        Allow="delete",
                    )
                return s3_object_version

            s3_object = self.objects.get(key)

            if not s3_object:
                raise NoSuchKey("The specified key does not exist.", Key=key)

            elif isinstance(s3_object, S3DeleteMarker):
                raise NoSuchKey(
                    "The specified key does not exist.",
                    Key=key,
                    DeleteMarker=True,
                )

        return s3_object


class S3Object:
    key: ObjectKey
    version_id: Optional[ObjectVersionId]
    size: Size
    etag: ETag
    user_metadata: Metadata  # TODO: check this?
    system_metadata: Metadata  # TODO: check this?
    last_modified: datetime
    expiry: Optional[datetime]
    expires: Optional[datetime]
    expiration: Optional[datetime]
    storage_class: StorageClass | ObjectStorageClass
    encryption: Optional[ServerSideEncryption]
    kms_key_id: Optional[SSEKMSKeyId]
    bucket_key_enabled: Optional[bool]
    checksum_algorithm: ChecksumAlgorithm
    checksum_value: str
    lock_mode: Optional[ObjectLockMode]
    lock_legal_status: Optional[ObjectLockLegalHoldStatus]
    lock_until: Optional[datetime]
    website_redirect_location: Optional[WebsiteRedirectLocation]
    acl: Optional[str]  # TODO: we need to change something here, how it's done?
    is_current: bool
    parts: Optional[list[tuple[int, int]]]

    def __init__(
        self,
        key: ObjectKey,
        etag: Optional[ETag] = None,
        size: Optional[int] = None,
        version_id: Optional[ObjectVersionId] = None,
        user_metadata: Optional[Metadata] = None,
        system_metadata: Optional[Metadata] = None,
        storage_class: StorageClass = StorageClass.STANDARD,
        expires: Optional[datetime] = None,
        expiration: Optional[datetime] = None,  # come from lifecycle
        checksum_algorithm: Optional[ChecksumAlgorithm] = None,
        checksum_value: Optional[str] = None,
        encryption: Optional[ServerSideEncryption] = None,  # inherit bucket
        kms_key_id: Optional[SSEKMSKeyId] = None,  # inherit bucket
        bucket_key_enabled: bool = False,  # inherit bucket
        lock_mode: Optional[ObjectLockMode] = None,  # inherit bucket
        lock_legal_status: Optional[ObjectLockLegalHoldStatus] = None,  # inherit bucket
        lock_until: Optional[datetime] = None,
        website_redirect_location: Optional[WebsiteRedirectLocation] = None,
        acl: Optional[str] = None,  # TODO
        expiry: Optional[datetime] = None,  # TODO
        # parts: Optional[list[tuple[str, str]]] = None,  # TODO: maybe remove?
    ):
        self.key = key
        self.user_metadata = {k.lower(): v for k, v in user_metadata.items()}
        # self.user_metadata = user_metadata
        self.system_metadata = system_metadata
        self.version_id = version_id
        self.storage_class = storage_class or StorageClass.STANDARD
        self.etag = etag
        self.size = size
        self.expires = expires
        self.checksum_algorithm = checksum_algorithm
        self.checksum_value = checksum_value
        self.encryption = encryption
        # TODO: validate the format, always store the ARN even if just the ID
        self.kms_key_id = kms_key_id
        self.bucket_key_enabled = bucket_key_enabled
        self.lock_mode = lock_mode
        self.lock_legal_status = lock_legal_status
        self.lock_until = lock_until
        self.acl = acl
        self.expiry = expiry
        self.expiration = expiration
        self.website_redirect_location = website_redirect_location
        self.is_current = True
        self.last_modified = datetime.now()
        # self._set_value_from_stream(value, buffer)
        # self.parts = parts
        self.parts = []

    def get_system_metadata_fields(self):
        headers = Headers()
        headers["LastModified"] = self.last_modified_rfc1123
        headers["ContentLength"] = str(self.size)
        headers["ETag"] = f'"{self.etag}"'
        if self.expires:
            headers["Expires"] = self.expires_rfc1123

        for metadata_key, metadata_value in self.system_metadata.items():
            headers[metadata_key] = metadata_value

        return headers

    @property
    def last_modified_iso8601(self) -> str:
        return iso_8601_datetime_without_milliseconds_s3(self.last_modified)  # type: ignore

    @property
    def last_modified_rfc1123(self) -> str:
        # Different datetime formats depending on how the key is obtained
        # https://github.com/boto/boto/issues/466
        return rfc_1123_datetime(self.last_modified)

    @property
    def expires_rfc1123(self) -> str:
        return rfc_1123_datetime(self.expires)

    @property
    def etag_header(self) -> str:
        return f'"{self.etag}"'


class S3DeleteMarker:
    key: ObjectKey
    version_id: str
    last_modified: datetime
    is_current: bool

    def __init__(self, key: ObjectKey, version_id: ObjectVersionId):
        self.key = key
        self.version_id = version_id
        self.last_modified = datetime.now()
        self.is_current = True


class S3Part:
    part_number: PartNumber
    etag: ETag
    last_modified: datetime
    size: int
    checksum_algorithm: Optional[ChecksumAlgorithm]
    checksum_value: Optional[str]

    def __init__(
        self,
        part_number: PartNumber,
        size: int,
        etag: ETag,
        checksum_algorithm: Optional[ChecksumAlgorithm] = None,
        checksum_value: Optional[str] = None,
    ):
        self.last_modified = datetime.now()
        self.part_number = part_number
        self.size = size
        self.etag = etag
        self.checksum_algorithm = checksum_algorithm
        self.checksum_value = checksum_value

    @property
    def etag_header(self) -> str:
        return f'"{self.etag}"'


class S3Multipart:
    parts: dict[PartNumber, S3Part]
    object: S3Object
    upload_id: MultipartUploadId
    checksum_value: Optional[str]
    initiated: datetime

    def __init__(
        self,
        key: ObjectKey,
        storage_class: StorageClass | ObjectStorageClass = StorageClass.STANDARD,
        expires: Optional[datetime] = None,
        expiration: Optional[datetime] = None,  # come from lifecycle
        checksum_algorithm: Optional[ChecksumAlgorithm] = None,
        encryption: Optional[ServerSideEncryption] = None,  # inherit bucket
        kms_key_id: Optional[SSEKMSKeyId] = None,  # inherit bucket
        bucket_key_enabled: bool = False,  # inherit bucket
        lock_mode: Optional[ObjectLockMode] = None,  # inherit bucket
        lock_legal_status: Optional[ObjectLockLegalHoldStatus] = None,  # inherit bucket
        lock_until: Optional[datetime] = None,
        website_redirect_location: Optional[WebsiteRedirectLocation] = None,
        acl: Optional[str] = None,  # TODO
        expiry: Optional[datetime] = None,  # TODO
        user_metadata: Optional[Metadata] = None,
        system_metadata: Optional[Metadata] = None,
        initiator: Optional[Owner] = None,
    ):
        self.id = token_urlsafe(10)  # TODO
        self.initiated = datetime.now()
        self.parts = {}
        self.initiator = initiator
        self.checksum_value = None
        self.object = S3Object(
            key=key,
            user_metadata=user_metadata,
            system_metadata=system_metadata,
            storage_class=storage_class or StorageClass.STANDARD,
            expires=expires,
            expiration=expiration,
            checksum_algorithm=checksum_algorithm,
            encryption=encryption,
            kms_key_id=kms_key_id,
            bucket_key_enabled=bucket_key_enabled,
            lock_mode=lock_mode,
            lock_legal_status=lock_legal_status,
            lock_until=lock_until,
            website_redirect_location=website_redirect_location,
            acl=acl,
            expiry=expiry,
        )

    # TODO: get the part list from the storage engine as well?
    def complete_multipart(
        self,
        parts: CompletedPartList,
        buffer: LockedSpooledTemporaryFile,
        parts_buffers: list[LockedSpooledTemporaryFile],
    ):
        last_part_index = len(parts) - 1
        if self.object.checksum_algorithm:
            checksum_key = f"Checksum{self.object.checksum_algorithm.upper()}"
        object_etag = hashlib.md5(usedforsecurity=False)

        pos = 0
        for index, part in enumerate(parts):
            part_number = part["PartNumber"]
            part_etag = part["ETag"]
            # TODO: verify checksum part, maybe from the algo?
            part_checksum = part.get(checksum_key) if self.object.checksum_algorithm else None

            s3_part = self.parts.get(part_number)
            # TODO: verify etag format here
            if not s3_part or s3_part.etag != part_etag.strip('"'):
                with buffer.lock:
                    buffer.seek(0)
                    buffer.truncate()
                # raise InvalidPart()
                raise
            # TODO: validate this?
            if part_checksum and part_checksum != s3_part.checksum_value:
                with buffer.lock:
                    buffer.seek(0)
                    buffer.truncate()
                # raise InvalidPart()
                raise
            if index != last_part_index and s3_part.size < S3_UPLOAD_PART_MIN_SIZE:
                with buffer.lock:
                    buffer.seek(0)
                    buffer.truncate()
                # raise EntityTooSmall()
                raise

            stream_value = parts_buffers[index]

            with stream_value.lock, buffer.lock:
                while data := stream_value.read(S3_CHUNK_SIZE):
                    buffer.write(data)
                    pos += len(data)

            object_etag.update(bytes.fromhex(s3_part.etag))
            self.object.parts.append((pos, s3_part.size))

        multipart_etag = f"{object_etag.hexdigest()}-{len(parts)}"
        self.object.etag = multipart_etag
        with buffer.lock:
            buffer.seek(0, 2)
            self.object.size = buffer.tell()
            buffer.seek(0)

        # free the space before the garbage collection, just to be faster
        # TODO: clear the parts_buffers in the provider from the StorageBackend
        # for part in self.parts.values():
        #     part.value.close()

        # now the full data should be in self.object.value, and the etag set
        # we can now properly retrieve the S3Object from the S3Multipart, set it as its own key
        # and delete the multipart
        # TODO: set the parts list to support `PartNumber` in GetObject !!


# TODO: use LockDict to prevent access during iteration? Copy works well I think, might be very slow?
# TODO: create base class?
class KeyStore:
    def __init__(self):
        self._store = {}

    def get(self, object_key: ObjectKey) -> S3Object | None:
        return self._store.get(object_key)

    def set(self, object_key: ObjectKey, s3_object: S3Object):
        self._store[object_key] = s3_object

    def pop(self, object_key: ObjectKey, default=None) -> S3Object | None:
        return self._store.pop(object_key, default)

    def is_empty(self) -> bool:
        return not self._store

    def values(self, *_, **__) -> list[S3Object | S3DeleteMarker]:
        return [value for value in self._store.values()]


class VersionedKeyStore:
    def __init__(self):
        self._store = defaultdict(dict)

    def get(
        self, object_key: ObjectKey, version_id: ObjectVersionId = None
    ) -> S3Object | S3DeleteMarker | None:
        if not version_id and (versions := self._store.get(object_key)):
            for version_id in reversed(versions):
                # Get the last set version for that key
                # TODO: test performance, could be cached if needed
                return versions.get(version_id)

        return self._store.get(object_key, {}).get(version_id)

    def set(self, object_key: ObjectKey, s3_object: S3Object | S3DeleteMarker):
        self._store[object_key][s3_object.version_id] = S3Object

    def pop(
        self, object_key: ObjectKey, version_id: ObjectVersionId = None, default=None
    ) -> S3Object | S3DeleteMarker | None:
        # TODO: implement this, what if no version_id? is it possible??
        versions = self._store.get(object_key)
        if not versions:
            # TODO: is that possible??
            return

        # TODO: pop key if empty?
        object_version = versions.pop(version_id, default)
        if not versions:
            self._store.pop(object_key)

        return object_version

    def values(self, with_versions: bool = False) -> list[S3Object | S3DeleteMarker]:
        if with_versions:
            return [object_version for values in self._store.values() for object_version in values]

        objects = []
        for object_key, versions in self._store.items():
            # we're getting the last set object in the versions dictionary
            for version_id in reversed(versions):
                current_object = versions[version_id]
                if isinstance(current_object, S3DeleteMarker):
                    break

                objects.append(versions[version_id])
                break

        return objects

    def is_empty(self) -> bool:
        return not self._store


#
#
# class _VersionedKeyStore(dict):  # type: ignore
#
#     """A modified version of Moto's `_VersionedKeyStore`"""
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._stored_versions = defaultdict(set)
#         self._lock = threading.RLock()
#
#     def get_versions_for_key(self, key: str) -> set[str]:
#         return self._stored_versions.get(key)
#
#     def __sgetitem__(self, key: str) -> list[S3Object | S3DeleteMarker]:
#         return super().__getitem__(key)
#
#     def __getitem__(self, key: str) -> S3Object | S3DeleteMarker:
#         return self.__sgetitem__(key)[-1]
#
#     def __setitem__(self, key: str, value: S3Object | S3DeleteMarker) -> None:
#         try:
#             current = self.__sgetitem__(key)
#             current.append(value)
#         except (KeyError, IndexError):
#             current = [value]
#
#         super().__setitem__(key, current)
#         if value.version_id:
#             self._stored_versions[key].add(value.version_id)
#
#     def pop(
#         self, key: str, default: S3Object | S3DeleteMarker = None
#     ) -> list[S3Object | S3DeleteMarker]:
#         value = super().pop(key, None)
#         if not value:
#             return [default]
#         self._stored_versions.pop(key, None)
#         return value
#
#     def get(self, key: str, default: S3Object | S3DeleteMarker = None) -> S3Object | S3DeleteMarker:
#         """
#         :param key: the ObjectKey
#         :param default: default return value if the key is not present
#         :return: the current (last version) S3Object or DeleteMarker
#         """
#         try:
#             return self[key]
#         except (KeyError, IndexError):
#             pass
#         return default
#
#     def setdefault(
#         self, key: str, default: S3Object | S3DeleteMarker = None
#     ) -> S3Object | S3DeleteMarker:
#         try:
#             return self[key]
#         except (KeyError, IndexError):
#             self[key] = default
#         return default
#
#     # TODO: could it actually append one version on top instead? hmm
#     def set_last_version(self, key: str, value: S3Object | S3DeleteMarker) -> None:
#         try:
#             self.__sgetitem__(key)[-1] = value
#         except (KeyError, IndexError):
#             super().__setitem__(key, [value])
#
#     def get_version(
#         self, key: str, version_id: str, default: S3Object | S3DeleteMarker = None
#     ) -> Optional[S3Object | S3DeleteMarker]:
#         s3_object_versions = self.getlist(key=key, default=default)
#         if not s3_object_versions:
#             return default
#
#         for s3_object_version in s3_object_versions:
#             if s3_object_version.version_id == version_id:
#                 return s3_object_version
#
#         return None
#
#     def getlist(
#         self, key: str, default: list[S3Object | S3DeleteMarker] = None
#     ) -> list[S3Object | S3DeleteMarker]:
#         try:
#             return self.__sgetitem__(key)
#         except (KeyError, IndexError):
#             pass
#         return default
#
#     def setlist(self, key: str, _list: list[S3Object | S3DeleteMarker]) -> None:
#         for value in _list:
#             if value.version_id:
#                 self._stored_versions[key].add(value.version_id)
#
#         if _list:
#             super().__setitem__(key, _list)
#         else:
#             self.pop(key)
#
#     def _iteritems(self) -> Iterator[tuple[str, S3Object | S3DeleteMarker]]:
#         for key in self._self_iterable():
#             yield key, self[key]
#
#     def _itervalues(self) -> Iterator[S3Object | S3DeleteMarker]:
#         for key in self._self_iterable():
#             yield self[key]
#
#     def _iterlists(self) -> Iterator[tuple[str, list[S3Object | S3DeleteMarker]]]:
#         for key in self._self_iterable():
#             yield key, self.getlist(key)
#
#     def item_size(self) -> int:
#
#         size = sum(key.size for key in self.values())
#         return size
#         # size = 0
#         # for val in self._self_iterable().values():
#         #     # TODO: not sure about that, especially storage values from tempfile?
#         #     # Should we iterate on key.size instead? and on every version? because we don't store diff or anything?
#         #     # not sure
#         #     size += val.size
#         #     # size += sys.getsizeof(val)
#         # return size
#
#     def _self_iterable(self) -> dict[str, S3Object | S3DeleteMarker]:
#         # TODO: locking
#         #  to enable concurrency, return a copy, to avoid "dictionary changed size during iteration"
#         #  TODO: look into replacing with a locking mechanism, potentially
#         return dict(self)
#
#     items = iteritems = _iteritems
#     lists = iterlists = _iterlists
#     values = itervalues = _itervalues


class S3StoreV2(BaseStore):
    buckets: dict[BucketName, S3Bucket] = CrossRegionAttribute(default=dict)
    global_bucket_map: dict[BucketName, AccountId] = CrossAccountAttribute(default=dict)


class BucketCorsIndexV2:
    def __init__(self):
        self._cors_index_cache = None
        self._bucket_index_cache = None

    @property
    def cors(self) -> dict[str, CORSConfiguration]:
        if self._cors_index_cache is None:
            self._cors_index_cache = self._build_index()
        return self._cors_index_cache

    @property
    def buckets(self) -> set[str]:
        if self._bucket_index_cache is None:
            self._bucket_index_cache = self._build_index()
        return self._bucket_index_cache

    def invalidate(self):
        self._cors_index_cache = None
        self._bucket_index_cache = None

    @staticmethod
    def _build_index() -> tuple[set[BucketName], dict[BucketName, CORSConfiguration]]:
        buckets = set()
        cors_index = {}
        for account_id, regions in s3_stores_v2.items():
            for bucket_name, bucket in regions[config.DEFAULT_REGION].buckets.items():
                bucket: S3Bucket
                buckets.add(bucket_name)
                if bucket.cors_rules is not None:
                    cors_index[bucket_name] = bucket.cors_rules

        return buckets, cors_index


class PartialStream(RawIOBase):
    def __init__(
        self, base_stream: IO[bytes] | LockedSpooledTemporaryFile, range_data: ParsedRange
    ):
        super().__init__()
        self._base_stream = base_stream
        self._pos = range_data.begin
        self._max_length = range_data.content_length

    def read(self, s: int = -1) -> bytes | None:
        if s is None or s < 0:
            amount = self._max_length
        else:
            amount = min(self._max_length, s)

        with self._base_stream.lock:
            self._base_stream.seek(self._pos)
            data = self._base_stream.read(amount)

        if not data:
            return b""
        read_amount = len(data)
        self._max_length -= read_amount
        self._pos += read_amount

        return data

    def readable(self) -> bool:
        return True


s3_stores_v2 = AccountRegionBundle[S3StoreV2]("s3", S3StoreV2)
