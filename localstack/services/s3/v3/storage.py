import abc
import base64
import hashlib
from collections import defaultdict
from io import BytesIO
from shutil import rmtree
from tempfile import SpooledTemporaryFile, mkdtemp
from threading import RLock
from typing import IO, Iterable, Optional, TypedDict

from readerwriterlock import rwlock

from localstack.aws.api.s3 import (
    BucketName,
    MultipartUploadId,
    ObjectKey,
    ObjectVersionId,
    PartNumber,
)
from localstack.services.s3.constants import S3_CHUNK_SIZE
from localstack.services.s3.utils import ChecksumHash, get_s3_checksum
from localstack.services.s3.v3.models import S3Multipart, S3Object, S3Part

# max file size for S3 objects kept in memory (500 KB by default)
# TODO: make it configurable
S3_MAX_FILE_SIZE_BYTES = 512 * 1024


def readinto_fileobj(
    value: IO[bytes],
    buffer: "LockedSpooledTemporaryFile",
    checksum_algorithm=None,
):
    if value is None:
        value = BytesIO()
    with buffer.readwrite_lock.gen_wlock():
        buffer.seek(0)
        buffer.truncate()
        # We have 2 cases:
        # The client gave a checksum value, we will need to compute the value and validate it against
        # or the client have an algorithm value only, and we need to compute the checksum
        if checksum_algorithm:
            checksum = get_s3_checksum(checksum_algorithm)

        etag = hashlib.md5(usedforsecurity=False)

        while data := value.read(S3_CHUNK_SIZE):
            buffer.write(data)
            etag.update(data)
            if checksum_algorithm:
                checksum.update(data)

        etag = etag.hexdigest()
        size = buffer.tell()
        buffer.seek(0)

        if checksum_algorithm:
            calculated_checksum = base64.b64encode(checksum.digest()).decode()
            return size, etag, calculated_checksum

        return size, etag, None


def read_from_fileobj_into_fileobj(
    src_fileobj: "LockedSpooledTemporaryFile",
    dest_fileobj: "LockedSpooledTemporaryFile",
    checksum_algorithm=None,
):
    with dest_fileobj.readwrite_lock.gen_wlock(), src_fileobj.readwrite_lock.gen_rlock():
        dest_fileobj.seek(0)
        dest_fileobj.truncate()
        # We have 2 cases:
        # The client gave a checksum value, we will need to compute the value and validate it against
        # or the client have an algorithm value only, and we need to compute the checksum
        if checksum_algorithm:
            checksum = get_s3_checksum(checksum_algorithm)

        etag = hashlib.md5(usedforsecurity=False)

        pos = 0
        while True:
            with src_fileobj.position_lock:
                src_fileobj.seek(pos)
                data = src_fileobj.read(S3_CHUNK_SIZE)

                if not data:
                    break

            dest_fileobj.write(data)
            pos += len(data)
            etag.update(data)
            if checksum_algorithm:
                checksum.update(data)

        etag = etag.hexdigest()
        size = dest_fileobj.tell()
        dest_fileobj.seek(0)

        if checksum_algorithm:
            calculated_checksum = base64.b64encode(checksum.digest()).decode()
            return size, etag, calculated_checksum

        return size, etag, None


class BaseStorageBackend(abc.ABC):
    """
    Base class abstraction for S3 Storage Backend. This allows decoupling between S3 metadata and S3 file objects
    """

    def create_bucket_directory(self, bucket_name: BucketName):
        raise NotImplementedError

    def delete_bucket_directory(self, bucket_name: BucketName):
        raise NotImplementedError

    def create_upload_directory(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        raise NotImplementedError

    def delete_upload_directory(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        raise NotImplementedError

    def get_key_fileobj(
        self,
        bucket_name: BucketName,
        object_key: ObjectKey,
        version_id: ObjectVersionId = None,
        in_place=False,
    ):
        """
        :param bucket_name
        :param object_key
        :param version_id
        :param in_place: the in_place parameters allows for in place copies, where we want override an existing file
        object at the same emplacement, but not return the previous fileobject (often opening both at the same time
        to read one into the other)
        :return:
        """
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
        return str(hash(f"{object_key}?{version_id or 'null'}"))

    @staticmethod
    def _get_fileobj_part(multipart_id: MultipartUploadId) -> str:
        return str(hash(multipart_id))


class BucketTemporaryFileSystem(TypedDict):
    keys: dict[str, "LockedSpooledTemporaryFile"]
    multiparts: dict[MultipartUploadId, dict[PartNumber, "LockedSpooledTemporaryFile"]]


class TemporaryStorageBackend(BaseStorageBackend):
    """
    This simulates a filesystem where S3 will store its assets
    The structure is the following:
    <bucket-name-1>/
    keys/
    ├─ <hash-key-1> -> fileobj
    ├─ <hash-key-2> -> fileobj
    multiparts/
    ├─ <upload-id-1>/
    │  ├─ <part-number-1> -> fileobj
    │  ├─ <part-number-2> -> fileobj
    """

    def __init__(self):
        self._filesystem: dict[BucketName, BucketTemporaryFileSystem] = defaultdict(
            self._get_bucket_filesystem
        )
        # this allows us to map bucket names to temporary directory name, to not have a flat structure inside the
        # temporary directory used by SpooledTemporaryFile
        self._directory_mapping: dict[str, str] = {}

    @staticmethod
    def _get_bucket_filesystem():
        return {"keys": {}, "multiparts": defaultdict(dict)}

    def create_bucket_directory(self, bucket_name: BucketName):
        """
        Create a temporary directory representing a bucket
        :param bucket_name
        """
        tmp_dir = mkdtemp()
        self._directory_mapping[bucket_name] = tmp_dir

    def delete_bucket_directory(self, bucket_name: BucketName):
        """
        Delete the temporary directory representing a bucket
        :param bucket_name
        """
        tmp_dir = self._directory_mapping.get(bucket_name)
        if tmp_dir:
            rmtree(tmp_dir, ignore_errors=True)

    def create_upload_directory(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        """
        Create a temporary
        :param bucket_name:
        :param upload_id:
        :return:
        """
        bucket_tmp_dir = self._directory_mapping.get(bucket_name)
        if not bucket_tmp_dir:
            self.create_bucket_directory(bucket_name)
            bucket_tmp_dir = self._directory_mapping.get(bucket_name)

        upload_tmp_dir = mkdtemp(dir=bucket_tmp_dir)
        self._directory_mapping[f"{bucket_name}/{upload_id}"] = upload_tmp_dir

    def delete_upload_directory(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        tmp_dir = self._directory_mapping.get(f"{bucket_name}/{upload_id}")
        if tmp_dir:
            rmtree(tmp_dir, ignore_errors=True)

    def get_key_fileobj(
        self,
        bucket_name: BucketName,
        object_key: ObjectKey,
        version_id: ObjectVersionId = None,
        in_place=False,
    ) -> "LockedSpooledTemporaryFile":
        key = self._get_fileobj_key(object_key, version_id)
        if in_place or not (
            fileobj := self._filesystem.get(bucket_name, {}).get("keys", {}).get(key)
        ):
            # if, for some race condition, bucket_tmp_dir is None, the SpooledFile will be in the default tmp dir
            # which is fine
            bucket_tmp_dir = self._directory_mapping.get(bucket_name)
            fileobj = LockedSpooledTemporaryFile(
                dir=bucket_tmp_dir, max_size=S3_MAX_FILE_SIZE_BYTES
            )
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
            upload_tmp_dir = self._directory_mapping.get(f"{bucket_name}/{upload_id}")
            fileobj = LockedSpooledTemporaryFile(
                dir=upload_tmp_dir, max_size=S3_MAX_FILE_SIZE_BYTES
            )
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


class LockedFileMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # this lock allows us to make `seek` and `read` operation as an atomic one, without an external reader
        # modifying the internal position of the stream
        self.position_lock = RLock()
        # these locks are for the read/write lock issues. No writer should modify the object while a reader is
        # currently iterating over it.
        # see:
        self.readwrite_lock = rwlock.RWLockWrite()


class LockedSpooledTemporaryFile(LockedFileMixin, SpooledTemporaryFile):
    def seekable(self) -> bool:
        return True


#
# class LockedStreamWrapper:
#     file: IO[bytes] | LockedFileMixin
#
#     def __init__(self, file: IO[bytes] | LockedFileMixin):
#         self.file = file
#
#     def __iter__(self):
#         pos = 0
#         with self.file.readwrite_lock.gen_rlock():
#             while True:
#                 # don't read more than the max content-length
#                 with self.file.position_lock:
#                     self.file.seek(pos)
#                     data = self.file.read(S3_CHUNK_SIZE)
#                 if not data:
#                     return b""
#
#                 read = len(data)
#                 pos += read
#
#                 yield data


# class LimitedLockedStreamWrapper(LockedStreamWrapper):
#     def __init__(self, file: IO[bytes] | LockedFileMixin, offset: int, max_length: int):
#         super().__init__(file)
#         self.position = offset
#         self.max_length = max_length
#
#     def __iter__(self):
#         with self.file.readwrite_lock.gen_rlock():
#             while True:
#                 # don't read more than the max content-length
#                 amount = min(self.max_length, S3_CHUNK_SIZE)
#                 with self.file.position_lock:
#                     self.file.seek(self.position)
#                     data = self.file.read(amount)
#                 if not data:
#                     return b""
#
#                 read = len(data)
#                 self.position += read
#                 self.max_length -= read
#
#                 yield data
#


# TODO: not tested, this is just a thought, need actual test
class LimitedIterableWrapper(Iterable[bytes]):
    def __init__(self, iterable: Iterable[bytes], max_length: int):
        self.iterable = iterable
        self.max_length = max_length

    def __iter__(self):
        for chunk in self.iterable:
            read = len(chunk)
            if self.max_length - read >= 0:
                self.max_length -= read
                yield chunk

            yield chunk[: self.max_length - read]

        return


"""
Operations manipulating storage directly or indirectly:
PutObject -> Object.write
GetObject -> Object.read wrapped around
UploadPart -> GetMultipart->WritePart? or ->Part.write
UploadPartCopy -> Object.read
CopyObject -> Object.read into Object.write
CompleteMultipartUpload -> GetMultipart->complete
"""


# TODO: naming? Shared between Object and Part
class S3StoredObject(abc.ABC):
    s3_object: S3Object

    def __init__(self, s3_object: S3Object | S3Part):
        self.s3_object = s3_object

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def write(self, s: IO[bytes]) -> int:
        pass

    @abc.abstractmethod
    def read(self, s: int = -1) -> bytes | None:
        pass

    @abc.abstractmethod
    def seek(self, offset: int, whence: int = 0) -> int:
        pass

    @abc.abstractmethod
    def checksum(self) -> Optional[str]:
        if not self.s3_object.checksum_algorithm:
            return None

    @abc.abstractmethod
    def __iter__(self) -> bytes:
        pass


class S3StoredMultipart(abc.ABC):
    parts: dict[PartNumber, S3StoredObject]
    s3_multipart: S3Multipart

    def __init__(self, s3_multipart: S3Multipart):
        self.s3_multipart = s3_multipart
        self.parts = {}

    @abc.abstractmethod
    def open(self, s3_part: S3Part) -> S3StoredObject:
        pass

    @abc.abstractmethod
    def remove_part(self, s3_part: S3Part):
        pass

    @abc.abstractmethod
    def complete_multipart(self) -> S3StoredObject:
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def copy_from_object(self, s3_part: S3Part, s3_object: S3Object, range_data):
        pass


class S3ObjectStore(abc.ABC):
    @abc.abstractmethod
    def open(self, bucket: BucketName, s3_object: S3Object) -> S3StoredObject:
        pass

    @abc.abstractmethod
    def remove(self, bucket: BucketName, s3_object: S3Object):
        pass

    @abc.abstractmethod
    def copy(
        self,
        src_bucket: BucketName,
        src_object: S3Object,
        dest_bucket: BucketName,
        dest_object: S3Object,
    ):
        pass

    @abc.abstractmethod
    def get_multipart(self, bucket: BucketName, upload_id: MultipartUploadId) -> S3StoredMultipart:
        pass


class EphemeralS3StoredObject(S3StoredObject):
    file: LockedSpooledTemporaryFile
    size: int
    _pos: int
    etag: Optional[str]
    checksum_hash: Optional[ChecksumHash]
    _checksum: Optional[str]

    def __init__(self, s3_object: S3Object | S3Part, file: LockedSpooledTemporaryFile):
        super().__init__(s3_object=s3_object)
        self.file = file
        self.size = 0
        self.etag = None
        self.checksum_hash = None
        self._checksum = None
        self._pos = 0
        if s3_object.checksum_algorithm:
            self.checksum_hash = get_s3_checksum(s3_object.checksum_algorithm)

    def read(self, s: int = -1) -> bytes | None:
        with self.file.position_lock:
            self.file.seek(self._pos)
            data = self.file.read(s)
        if not data:
            return b""

        read = len(data)
        self._pos += read
        return data

    def seek(self, offset: int, whence: int = 0) -> int:
        # original_pos = self.file.tell()
        with self.file.position_lock:
            self.file.seek(offset, whence)
            self._pos = self.file.tell()
            # self.file.seek(original_pos)

        return self._pos

    def write(self, stream: IO[bytes]) -> int:
        if stream is None:
            stream = BytesIO()

        file = self.file
        with file.readwrite_lock.gen_wlock():
            file.seek(0)
            file.truncate()

            etag = hashlib.md5(usedforsecurity=False)

            while data := stream.read(S3_CHUNK_SIZE):
                file.write(data)
                etag.update(data)
                if self.checksum_hash:
                    self.checksum_hash.update(data)

            etag = etag.hexdigest()
            self.size = self.s3_object.size = file.tell()
            self.etag = self.s3_object.etag = etag

            file.seek(0)
            self._pos = 0

            return self.size

    def close(self):
        return self.file.close()

    def checksum(self) -> Optional[str]:
        if not self.checksum_hash:
            return
        if not self._checksum:
            self._checksum = base64.b64encode(self.checksum_hash.digest()).decode()

        return self._checksum

    def __iter__(self) -> bytes:
        with self.file.readwrite_lock.gen_rlock():
            while data := self.read(S3_CHUNK_SIZE):
                if not data:
                    return b""

                yield data


class EphemeralS3StoredMultipart(S3StoredMultipart):
    upload_dir: str

    def __init__(self, s3_multipart: S3Multipart, upload_dir: str):
        # self.s3_multipart = s3_multipart
        # self.bucket = bucket
        # self.parts = {}
        super().__init__(s3_multipart)
        self.upload_dir = upload_dir

    def open(self, s3_part: S3Part) -> EphemeralS3StoredObject:
        if not (stored_part := self.parts.get(s3_part.part_number)):
            file = LockedSpooledTemporaryFile(dir=self.upload_dir, max_size=S3_MAX_FILE_SIZE_BYTES)
            stored_part = EphemeralS3StoredObject(s3_part, file)
            self.parts[s3_part.part_number] = stored_part

        return stored_part

    @abc.abstractmethod
    def remove_part(self, s3_part: S3Part):
        pass

    @abc.abstractmethod
    def complete_multipart(self) -> EphemeralS3StoredObject:
        pass

    def close(self):
        pass

    @abc.abstractmethod
    def copy_from_object(self, s3_part: S3Part, s3_object: S3Object, range_data):
        pass

    def _get_part(self, s3_part: S3Part) -> EphemeralS3StoredObject:
        if not (stored_part := self.parts.get(s3_part.part_number)):
            file = LockedSpooledTemporaryFile(dir=self.upload_dir, max_size=S3_MAX_FILE_SIZE_BYTES)
            stored_part = EphemeralS3StoredObject(s3_part, file)
            self.parts[s3_part.part_number] = stored_part

        return stored_part


class BucketTemporaryFileSystem2(TypedDict):
    keys: dict[str, LockedSpooledTemporaryFile]
    multiparts: dict[MultipartUploadId, EphemeralS3StoredMultipart]


class EphemeralS3ObjectStore(S3ObjectStore):
    """
    This simulates a filesystem where S3 will store its assets
    The structure is the following:
    <bucket-name-1>/
    keys/
    ├─ <hash-key-1> -> fileobj
    ├─ <hash-key-2> -> fileobj
    multiparts/
    ├─ <upload-id-1>/
    │  ├─ <part-number-1> -> fileobj
    │  ├─ <part-number-2> -> fileobj
    """

    def __init__(self):
        self._filesystem: dict[BucketName, BucketTemporaryFileSystem2] = defaultdict(
            lambda: {"keys": {}, "multiparts": {}}
        )
        # this allows us to map bucket names to temporary directory name, to not have a flat structure inside the
        # temporary directory used by SpooledTemporaryFile
        self._directory_mapping: dict[str, str] = {}

    def open(self, bucket: BucketName, s3_object: S3Object) -> S3StoredObject:
        key = self._key_from_s3_object(s3_object)
        if not (file := self._get_object_file(bucket, key)):
            if not (bucket_tmp_dir := self._directory_mapping.get(bucket)):
                bucket_tmp_dir = self._create_bucket_directory(bucket)

            file = LockedSpooledTemporaryFile(dir=bucket_tmp_dir, max_size=S3_MAX_FILE_SIZE_BYTES)
            self._filesystem[bucket]["keys"][key] = file

        return EphemeralS3StoredObject(s3_object=s3_object, file=file)

    def remove(self, bucket: BucketName, s3_object: S3Object):
        if keys := self._filesystem.get(bucket, {}).get("keys", {}):
            key = self._key_from_s3_object(s3_object)
            keys.pop(key, None)

        # if the bucket is now empty after removing, we can delete the directory
        if not keys and not self._filesystem.get(bucket, {}).get("multiparts"):
            self._delete_bucket_directory(bucket)

    def copy(
        self,
        src_bucket: BucketName,
        src_object: S3Object,
        dest_bucket: BucketName,
        dest_object: S3Object,
    ):
        pass

    def get_multipart(
        self, bucket: BucketName, s3_multipart: S3Multipart
    ) -> EphemeralS3StoredMultipart:
        upload_key = self._resolve_upload_directory(bucket, s3_multipart.id)
        if not (multipart := self._get_multipart(bucket, upload_key)):

            upload_dir = self._create_upload_directory(bucket, s3_multipart.id)

            multipart = EphemeralS3StoredMultipart(s3_multipart, upload_dir)
            self._filesystem[bucket]["multiparts"][upload_key] = multipart

        return multipart

    def remove_multipart(self, bucket: BucketName, s3_multipart: S3Multipart):
        if multiparts := self._filesystem.get(bucket, {}).get("multiparts", {}):
            upload_key = self._resolve_upload_directory(bucket, s3_multipart.id)
            multiparts.pop(upload_key, None)

        # if the bucket is now empty after removing, we can delete the directory
        if not multiparts and not self._filesystem.get(bucket, {}).get("keys"):
            self._delete_bucket_directory(bucket)

    @staticmethod
    def _key_from_s3_object(s3_object: S3Object) -> str:
        return str(hash(f"{s3_object.key}?{s3_object.version_id or 'null'}"))

    def _get_object_file(self, bucket: BucketName, key: str) -> LockedSpooledTemporaryFile | None:
        return self._filesystem.get(bucket, {}).get("keys", {}).get(key)

    def _get_multipart(self, bucket: BucketName, upload_key: str) -> S3StoredMultipart | None:
        return self._filesystem.get(bucket, {}).get("multiparts", {}).get(upload_key)

    @staticmethod
    def _resolve_upload_directory(bucket_name: BucketName, upload_id: MultipartUploadId) -> str:
        return f"{bucket_name}/{upload_id}"

    def _create_bucket_directory(self, bucket_name: BucketName) -> str:
        """
        Create a temporary directory representing a bucket
        :param bucket_name
        """
        tmp_dir = mkdtemp()
        self._directory_mapping[bucket_name] = tmp_dir
        return tmp_dir

    def _delete_bucket_directory(self, bucket_name: BucketName):
        """
        Delete the temporary directory representing a bucket
        :param bucket_name
        """
        tmp_dir = self._directory_mapping.get(bucket_name)
        if tmp_dir:
            rmtree(tmp_dir, ignore_errors=True)

    def _create_upload_directory(
        self, bucket_name: BucketName, upload_id: MultipartUploadId
    ) -> str:
        """
        Create a temporary
        :param bucket_name:
        :param upload_id:
        :return:
        """
        bucket_tmp_dir = self._directory_mapping.get(bucket_name)
        if not bucket_tmp_dir:
            self._create_bucket_directory(bucket_name)
            bucket_tmp_dir = self._directory_mapping.get(bucket_name)

        upload_tmp_dir = mkdtemp(dir=bucket_tmp_dir)
        self._directory_mapping[f"{bucket_name}/{upload_id}"] = upload_tmp_dir
        return upload_tmp_dir

    def _delete_upload_directory(self, bucket_name: BucketName, upload_id: MultipartUploadId):
        tmp_dir = self._directory_mapping.get(f"{bucket_name}/{upload_id}")
        if tmp_dir:
            rmtree(tmp_dir, ignore_errors=True)
