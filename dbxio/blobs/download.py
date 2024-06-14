from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tenacity import retry, stop_after_attempt, wait_fixed

if TYPE_CHECKING:
    from dbxio.utils.object_storage import ObjectStorage


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def download_blob_tree(object_storage: 'ObjectStorage', local_path: Path, prefix_path: Optional[str] = None):
    for blob in object_storage.list_blobs(prefix=prefix_path):
        if blob.content_settings.content_type is None:
            # it's a directory, create it
            Path(local_path / blob.name).mkdir(parents=True, exist_ok=True)
            continue

        object_storage.download_blob_to_file(blob.name, local_path / blob.name)
