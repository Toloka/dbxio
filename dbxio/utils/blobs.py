from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbxio.core.cloud.client.object_storage import ObjectStorageClient


def blobs_gc(blobs: list[str], object_storage_client: 'ObjectStorageClient'):
    for blob in blobs:
        object_storage_client.try_delete_blob(blob)


@contextmanager
def blobs_registries(
    object_storage_client: 'ObjectStorageClient',
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
            blobs_gc(blobs, object_storage_client)
        if not keep_metablobs:
            blobs_gc(metablobs, object_storage_client)
