from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tenacity import retry, stop_after_attempt, wait_fixed

if TYPE_CHECKING:
    from dbxio.core.cloud.client.object_storage import ObjectStorageClient


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def download_blob_tree(
    object_storage_client: 'ObjectStorageClient',
    local_path: Path,
    prefix_path: Optional[str] = None,
):
    for blob in object_storage_client.list_blobs(prefix=prefix_path):
        is_dir = object_storage_client.is_directory(blob.name)
        if blob.name == prefix_path and is_dir:
            # prefix path is subdir, skip it on first iteration
            continue

        relative_blob_path = blob.name[len(prefix_path) + 1 :] if prefix_path else blob.name
        if not relative_blob_path:
            # if the prefix path is full path to one file
            assert object_storage_client.blobs_path, 'blobs_path is not set'
            relative_blob_path = blob.name[len(object_storage_client.blobs_path) + 1 :]

        if is_dir:
            if not blob.name.startswith(f'{prefix_path}/'):
                # we found a directory with the same prefix, but it's not from our subtree
                continue
            # it's a directory, create it
            Path(local_path / relative_blob_path).mkdir(parents=True, exist_ok=True)
            continue

        object_storage_client.download_blob_to_file(blob.name, local_path / relative_blob_path)
