import abc
from collections import defaultdict
from tempfile import SpooledTemporaryFile
from threading import RLock
from typing import TypedDict

from localstack.aws.api.s3 import (
    BucketName,
    MultipartUploadId,
    ObjectKey,
    ObjectVersionId,
    PartNumber,
)

# max file size for S3 objects kept in memory (500 KB by default)
# TODO: make it configurable
S3_MAX_FILE_SIZE_BYTES = 512 * 1024


class BaseStorageBackend(abc.ABC):
    """
    Base class abstraction for S3 Storage Backend. This allows decoupling between S3 metadata and S3 file objects
    """

    def get_key_fileobj(
        self, bucket_name: BucketName, object_key: ObjectKey, version_id: ObjectVersionId = None
    ):
        raise NotImplementedError

    def delete_key_fileobj(
        self, bucket_name: BucketName, object_key: ObjectKey, version_id: str = None
    ):
        raise NotImplementedError

    def get_part_fileobj(
        self, bucket_name: BucketName, upload_id: MultipartUploadId, part_number: PartNumber
    ):
        raise NotImplementedError

    def delete_part_fileobj(
        self, bucket_name: BucketName, upload_id: MultipartUploadId, part_number: PartNumber
    ):
        raise NotImplementedError

    def get_list_parts_fileobjs(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        raise NotImplementedError

    def delete_multipart_fileobjs(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        raise NotImplementedError

    @staticmethod
    def _get_fileobj_key(object_key: ObjectKey, version_id: ObjectVersionId = None) -> str:
        return str(hash(f"{object_key}?{version_id}"))

    @staticmethod
    def _get_fileobj_part(multipart_id: MultipartUploadId) -> str:
        # TODO: might not need to hash it? just use the upload_id?
        return str(hash(multipart_id))


class BucketTemporaryFileSystem(TypedDict):
    keys: dict[str, "LockedSpooledTemporaryFile"]
    multiparts: dict[MultipartUploadId, dict[PartNumber, "LockedSpooledTemporaryFile"]]


class TemporaryStorageBackend(BaseStorageBackend):
    """
    This simulates a filesystem where S3 will store its assets
    The structure is the following:
    - <bucket-name-1>
    |- <keys>
    |  |-<hash-key-1> -> fileobj
    |  |-<hash-key-2> -> fileobj
    |- <multiparts>
    |  |-<upload_id-1>
    |  |  |-<part-number-1> -> fileobj
    |  |  |-<part-number-2> -> fileobj
    """

    def __init__(self):
        self._filesystem: dict[BucketName, BucketTemporaryFileSystem] = defaultdict(
            self._get_bucket_filesystem
        )

    @staticmethod
    def _get_bucket_filesystem():
        return {"keys": {}, "multiparts": defaultdict(dict)}

    def get_key_fileobj(
        self, bucket_name: BucketName, object_key: ObjectKey, version_id: ObjectVersionId = None
    ) -> "LockedSpooledTemporaryFile":
        key = self._get_fileobj_key(object_key, version_id)
        if not (fileobj := self._filesystem.get(bucket_name, {}).get("keys", {}).get(key)):
            # fileobj = LockedSpooledTemporaryFile(dir=bucket_name, max_size=S3_MAX_FILE_SIZE_BYTES)
            fileobj = LockedSpooledTemporaryFile(max_size=S3_MAX_FILE_SIZE_BYTES)
            self._filesystem[bucket_name]["keys"][key] = fileobj

        return fileobj

    def delete_key_fileobj(
        self, bucket_name: BucketName, object_key: ObjectKey, version_id: str = None
    ):
        key = self._get_fileobj_key(object_key, version_id)
        if fileobj := self._filesystem.get(bucket_name, {}).get("keys", {}).get(key):
            fileobj.close()

        self._filesystem.get(bucket_name, {}).get("keys", {}).pop(key, None)

    def get_part_fileobj(
        self, bucket_name: BucketName, upload_id: MultipartUploadId, part_number: PartNumber
    ) -> "LockedSpooledTemporaryFile":
        key = self._get_fileobj_part(upload_id)
        if not (
            fileobj := self._filesystem.get(bucket_name, {})
            .get("multiparts", {})
            .get(key, {})
            .get(part_number)
        ):
            # TODO: check dir param for upload_id too?
            # fileobj = LockedSpooledTemporaryFile(dir=bucket_name, max_size=S3_MAX_FILE_SIZE_BYTES)
            fileobj = LockedSpooledTemporaryFile(max_size=S3_MAX_FILE_SIZE_BYTES)
            self._filesystem[bucket_name]["multiparts"][key][part_number] = fileobj

        return fileobj

    def delete_part_fileobj(
        self, bucket_name: BucketName, upload_id: MultipartUploadId, part_number: PartNumber
    ):
        key = self._get_fileobj_part(upload_id)
        if (
            fileobj := self._filesystem.get(bucket_name, {})
            .get("multiparts", {})
            .get(key, {})
            .get(part_number)
        ):
            fileobj.close()

        self._filesystem.get(bucket_name, {}).get("multiparts", {}).get(key, {}).pop(
            part_number, None
        )

    def get_list_parts_fileobjs(
        self, bucket_name: BucketName, upload_id: MultipartUploadId
    ) -> list["LockedSpooledTemporaryFile"]:
        key = self._get_fileobj_part(upload_id)
        parts = self._filesystem.get(bucket_name, {}).get("multiparts", {}).get(key, {})
        return [fileobj for part_number, fileobj in sorted(parts.items())]

    def delete_multipart_fileobjs(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        key = self._get_fileobj_part(upload_id)
        parts = self._filesystem.get(bucket_name, {}).get("multiparts", {}).get(key, {})
        for fileobj in parts.values():
            fileobj.close()

        self._filesystem.get(bucket_name, {}).get("multiparts", {}).pop(key, None)


class LockedSpooledTemporaryFile(SpooledTemporaryFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = RLock()


class FilesystemStorageBackend(BaseStorageBackend):
    pass


class LockedSpooledFile:  # TODO: implement this for persistence
    pass
