import re
from io import IOBase
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Iterator, Optional, Union

import attrs
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobLeaseClient, BlobServiceClient
from cachetools import TTLCache

from dbxio.core.cloud.azure import AZURE_ClOUD_PROVIDER_NAME
from dbxio.core.cloud.client.object_storage import ObjectStorageClient
from dbxio.core.cloud.exceptions import BlobModificationError
from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core.auth import AZ_CRED_PROVIDER_TYPE

logger = get_logger()


@attrs.define
class _AzureBlobStorageClientImpl(ObjectStorageClient):
    container_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    storage_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    blobs_path: str = attrs.field(default='', validator=attrs.validators.instance_of(str))
    domain_name: str = attrs.field(default='dfs.core.windows.net', validator=attrs.validators.instance_of(str))
    credential_provider: 'AZ_CRED_PROVIDER_TYPE' = attrs.field(default=DefaultAzureCredential())
    blob_service_client: BlobServiceClient = attrs.field()

    scheme = 'abfss'
    cloud_provider_name = AZURE_ClOUD_PROVIDER_NAME
    # container_name@storage_name.domain_name/blobs_path
    url_regex = re.compile(
        r'(?P<container_name>[^@]+)@(?P<storage_name>[^.]+)\.(?P<domain_name>[^/]+)/(?P<blobs_path>.*)$'
    )
    _cache: TTLCache = attrs.Factory(lambda: TTLCache(maxsize=1024, ttl=60 * 15))

    @blob_service_client.default
    def _blob_service_client_factory(self) -> BlobServiceClient:
        return BlobServiceClient(
            account_url=f'https://{self.storage_name}.blob.core.windows.net',
            credential=self.credential_provider,
        )

    def to_url(self) -> str:
        return f'{self.scheme}://{self.container_name}@{self.storage_name}.{self.domain_name}/{self.blobs_path}'

    @property
    def account_url(self) -> str:
        return f'https://{self.storage_name}.blob.core.windows.net'

    def list_blobs(self, prefix: Optional[str] = None, **kwargs) -> Iterator:
        container_client = self.blob_service_client.get_container_client(self.container_name)
        return container_client.list_blobs(name_starts_with=prefix, **kwargs)

    def download_blob(self, blob_name: str) -> bytes:
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        return blob_client.download_blob().readall()

    def download_blob_to_file(self, blob_name: str, file_path: Union[str, Path]) -> None:
        with open(file_path, 'wb') as f:
            f.write(self.download_blob(blob_name))

    def break_lease(self, blob_name: str) -> None:
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        BlobLeaseClient(client=blob_client).break_lease()  # type: ignore

    def lock_blob(self, blob_name: str, force: bool = False):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        if force:
            self.break_lease(blob_name)
        try:
            blob_client.acquire_lease()
        except ResourceExistsError as e:
            raise BlobModificationError(f'Failed to lock blob {blob_name}') from e

    def upload_blob(self, blob_name: str, data: Union[bytes, IOBase, BinaryIO], overwrite: bool = False, **kwargs):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        try:
            blob_client.upload_blob(data, **kwargs, overwrite=overwrite)
        except ResourceExistsError as e:
            raise BlobModificationError(f'Failed to upload blob {blob_name}') from e

    def try_delete_blob(self, blob_name: str) -> None:
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        lease_client = BlobLeaseClient(client=blob_client)  # type: ignore

        try:
            lease_client.break_lease()
        except (ResourceExistsError, ResourceNotFoundError):
            pass

        try:
            blob_client.delete_blob()
        except ResourceNotFoundError:
            pass
