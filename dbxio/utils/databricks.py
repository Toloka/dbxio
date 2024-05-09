import re
from enum import Enum

ABS_NAME_REGEX = re.compile(r'@([^@.]+)')


class ClusterType(Enum):
    ALL_PURPOSE = 'ALL_PURPOSE'
    SQL_WAREHOUSE = 'SQL_WAREHOUSE'


def get_storage_name_from_external_location(storage_url: str, external_location: str) -> str:
    """
    Fetches the storage name from a storage URL associated with an external location.
    Storage URL has the pattern: abfss://<container>@<storage>.dfs.core.windows.net/
    """
    _match = ABS_NAME_REGEX.search(storage_url)
    if _match:
        return _match.group(1)

    raise ValueError(
        f'Could not extract storage name for external location. ' f'Got: {external_location=}, {storage_url=}'
    )


def compile_full_storage_location_path(container_name: str, storage_name: str, blobs_path: str) -> str:
    """
    External storage location has the pattern: abfss://<container>@<storage_name>.dfs.core.windows.net/path/to/blobs
    """
    return f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/{blobs_path}'
