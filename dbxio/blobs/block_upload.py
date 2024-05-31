import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Union

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobLeaseClient, BlobType

from dbxio.utils.logging import get_logger

_HASHSUM_SUFFIX = '_HASHSUM'
_SUCCESS_SUFFIX = '_SUCCESS'
_LOCK_SUFFIX = '_LOCK'

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient, ContainerClient

logger = get_logger()


def _local_path_to_blob_name(file_path: Path, local_path: Path, operation_uuid: str) -> str:
    relative_path = file_path.relative_to(local_path) if file_path != local_path else file_path.name
    return f'{operation_uuid}/{relative_path}'


def _get_file_hash(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b''):
            sha256.update(byte_block)
    return sha256.hexdigest()


def _blob_exists(container_client: 'ContainerClient', blob_name: str, target_hashsum: str) -> bool:
    """
    Checks that blob exists in the container. It's also required to check that hashsums are equal and file with
    suffix _SUCCESS exists.
    """
    blobs = [blob.name for blob in container_client.list_blobs(name_starts_with=blob_name)]
    if f'{blob_name}{_SUCCESS_SUFFIX}' not in blobs or f'{blob_name}{_HASHSUM_SUFFIX}' not in blobs:
        logger.debug(f'Blob {blob_name} does not exist (no SUCCESS or HASHSUM file)')
        return False
    saved_hashsum = container_client.download_blob(f'{blob_name}{_HASHSUM_SUFFIX}').readall().decode()
    return saved_hashsum == target_hashsum


def _lock_blob(blob_name: str, blob_service_client: 'BlobServiceClient', container_name: str, force: bool = False):
    """
    Locks blob by creating a lease on <blob_name>_LOCK file.
    If any other process tries to upload the same file, it will fail to acquire the lease.
    If force is True, it will break the lease and acquire it again.
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f'{blob_name}{_LOCK_SUFFIX}')
    try:
        blob_client.upload_blob(b'', overwrite=True)
    except ResourceExistsError as e:
        if force:
            BlobLeaseClient(client=blob_client).break_lease()  # type: ignore
        else:
            raise e
    blob_client.acquire_lease()

    logger.debug(f'Lock is acquired for {blob_name}')


def upload_file(
    path: Union[str, Path],
    local_path: Union[str, Path],
    blob_service_client: 'BlobServiceClient',
    container_name: str,
    blobs: list[str],
    metablobs: list[str],
    operation_uuid: str,
    max_concurrency: int = 1,
    force: bool = False,
) -> str:
    """
    Uploads file to the Azure Blob Storage container with guarantees that only one process can upload the same file at
    the same time.
    It also checks that the file is not uploaded yet by comparing hashsums.
    """
    path = Path(path)
    local_path = Path(local_path)
    container_client = blob_service_client.get_container_client(container_name)
    logger.debug(f'Using {max_concurrency} threads for uploading {path}')

    file_hash = _get_file_hash(path)
    blob_name = _local_path_to_blob_name(path, local_path, operation_uuid)
    metablobs.append(f'{blob_name}{_LOCK_SUFFIX}')
    metablobs.append(f'{blob_name}{_SUCCESS_SUFFIX}')
    metablobs.append(f'{blob_name}{_HASHSUM_SUFFIX}')

    blobs.append(blob_name)

    if _blob_exists(container_client, blob_name, file_hash):
        return blob_name

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    _lock_blob(blob_name, blob_service_client, container_name, force=force)

    with open(path, 'rb') as data:
        blob_client.upload_blob(
            data,
            blob_type=BlobType.BLOCKBLOB,
            overwrite=True,
            max_concurrency=int(max_concurrency),
        )

    container_client.upload_blob(f'{blob_name}{_SUCCESS_SUFFIX}', b'', overwrite=True)
    logger.debug(f'SUCCESS file is uploaded for {blob_name}')
    container_client.upload_blob(f'{blob_name}{_HASHSUM_SUFFIX}', file_hash.encode(), overwrite=True)
    logger.debug(f'HASHSUM file is uploaded for {blob_name}')

    logger.info(f'Successfully uploaded {path} to {container_name}/{blob_name}')

    return blob_name
