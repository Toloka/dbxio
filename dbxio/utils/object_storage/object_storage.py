import re
from abc import ABC, abstractmethod
from io import IOBase
from pathlib import Path
from typing import BinaryIO, Iterator, Optional, Type


class ObjectStorage(ABC):
    url_regex: re.Pattern

    # azure properties
    container_name: str
    storage_name: str
    domain_name: Optional[str]
    blobs_path: Optional[str]

    # s3/gs properties
    bucket_name: str
    object_key: str

    def __init__(self, **kwargs):
        raise NotImplementedError

    @property
    @abstractmethod
    def scheme(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def credential_provider(self):
        raise NotImplementedError

    @abstractmethod
    def to_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def try_delete_blob(self, blob_name: str):
        raise NotImplementedError

    @abstractmethod
    def list_blobs(self, prefix: Optional[str] = None, **kwargs) -> Iterator:
        raise NotImplementedError

    @abstractmethod
    def download_blob(self, blob_name: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def download_blob_to_file(self, blob_name: str, file_path: str | Path) -> None:
        raise NotImplementedError

    @abstractmethod
    def upload_blob(self, blob_name: str, data: bytes | IOBase | BinaryIO, overwrite: bool = False, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def lock_blob(self, blob_name: str, force: bool = False):
        raise NotImplementedError

    @abstractmethod
    def break_lease(self, blob_name: str) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_storage_impl(scheme: str) -> 'Type[ObjectStorage]':
        from dbxio.utils.object_storage._aws import _S3StorageImpl
        from dbxio.utils.object_storage._azure import _AzureBlobStorageImpl
        from dbxio.utils.object_storage._gcp import _GCStorageImpl

        if scheme == _AzureBlobStorageImpl.scheme:
            return _AzureBlobStorageImpl
        if scheme == _S3StorageImpl.scheme:
            return _S3StorageImpl
        if scheme == _GCStorageImpl.scheme:
            return _GCStorageImpl

        raise ValueError(f'Unsupported scheme: {scheme}')

    @classmethod
    def from_url(cls, storage_url: str) -> 'ObjectStorage':
        scheme, url = storage_url.split('://')
        _storage_impl = cls._get_storage_impl(scheme)
        _match = _storage_impl.url_regex.match(url)
        if _match:
            return _storage_impl(**_match.groupdict())

        raise ValueError(f'Invalid Azure Blob Storage URL: {url}')

    @classmethod
    def from_storage_options(cls, scheme: str, **storage_options) -> 'ObjectStorage':
        _storage_impl = cls._get_storage_impl(scheme)
        return _storage_impl(**storage_options)
