from contextlib import contextmanager
from typing import TYPE_CHECKING

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobLeaseClient, BlobServiceClient

if TYPE_CHECKING:
    from dbxio.core.auth import AZ_CRED_PROVIDER_TYPE


def compile_storage_account_url(storage_name: str) -> str:
    return f'https://{storage_name}.blob.core.windows.net'


def get_blob_servie_client(
    storage_name: str,
    az_cred_provider: 'AZ_CRED_PROVIDER_TYPE',
):
    return BlobServiceClient(
        account_url=compile_storage_account_url(storage_name),
        credential=az_cred_provider,
    )


def _try_delete_blob(blob_name: str, blob_service_client: 'BlobServiceClient', container_name: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    lease_client = BlobLeaseClient(client=blob_client)  # type: ignore

    try:
        lease_client.break_lease()
    except (ResourceExistsError, ResourceNotFoundError):
        pass

    try:
        blob_client.delete_blob()
    except ResourceNotFoundError:
        pass


def blobs_gc(blobs: list[str], blob_service_client: 'BlobServiceClient', container_name: str):
    for blob in blobs:
        _try_delete_blob(blob, blob_service_client, container_name)


@contextmanager
def blobs_registries(
    blob_service_client: 'BlobServiceClient',
    container_name: str,
    keep_blobs: bool = False,
    keep_metablobs: bool = False,
):
    """
    Context manager to manage the blobs and metablobs lists.
    """
    blobs: list[str] = []
    metablobs: list[str] = []
    try:
        yield blobs, metablobs
    finally:
        if not keep_blobs:
            blobs_gc(blobs, blob_service_client, container_name)
        if not keep_metablobs:
            blobs_gc(metablobs, blob_service_client, container_name)
