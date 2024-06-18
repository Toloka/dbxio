import re
from abc import ABC, abstractmethod
from io import IOBase
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Iterator, Optional, Type, Union

if TYPE_CHECKING:
    from dbxio.core.cloud import CloudProvider


class ObjectStorageClient(ABC):
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
    def cloud_provider_name(self) -> str:
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
    def download_blob_to_file(self, blob_name: str, file_path: Union[str, Path]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upload_blob(self, blob_name: str, data: Union[bytes, IOBase, BinaryIO], overwrite: bool = False, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def lock_blob(self, blob_name: str, force: bool = False):
        raise NotImplementedError

    @abstractmethod
    def break_lease(self, blob_name: str) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_storage_impl(
        value: 'Union[str | CloudProvider]',
        by_scheme: bool = False,
        by_cloud_provider: bool = False,
    ) -> 'Type[ObjectStorageClient]':
        from dbxio.core.cloud.aws.object_storage import _S3StorageClientImpl
        from dbxio.core.cloud.azure.object_storage import _AzureBlobStorageClientImpl
        from dbxio.core.cloud.gcp.object_storage import _GCStorageClientImpl

        assert by_scheme or by_cloud_provider, 'Either by_scheme or by_cloud_provider should be set to True'

        attr_name = 'scheme' if by_scheme else 'cloud_provider_name'
        if value == getattr(_AzureBlobStorageClientImpl, attr_name):
            return _AzureBlobStorageClientImpl
        if value == getattr(_S3StorageClientImpl.scheme, attr_name):
            return _S3StorageClientImpl
        if value == getattr(_GCStorageClientImpl.scheme, attr_name):
            return _GCStorageClientImpl

        raise ValueError(f'Unsupported storage type: {value}')

    @classmethod
    def from_url(cls, storage_url: str) -> 'ObjectStorageClient':
        scheme, url = storage_url.split('://')
        _storage_impl = cls._get_storage_impl(scheme, by_scheme=True)
        _match = _storage_impl.url_regex.match(url)
        if _match:
            return _storage_impl(**_match.groupdict())

        raise ValueError(f'Invalid Storage URL: {url}')

    @classmethod
    def from_storage_options(cls, cloud_provider: 'CloudProvider', **storage_options) -> 'ObjectStorageClient':
        _storage_impl = cls._get_storage_impl(cloud_provider, by_cloud_provider=True)
        return _storage_impl(**storage_options)
