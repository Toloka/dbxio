import pytest


def test_get_storage_name_from_external_location():
    from dbxio.utils.databricks import get_storage_name_from_external_location

    def _get_storage_url(container: str, storage: str) -> str:
        return f'abfss://{container}@{storage}.dfs.core.windows.net/'

    external_location = 'storage_name'

    assert (
        get_storage_name_from_external_location(_get_storage_url(external_location, 'storage_name'), external_location)
        == 'storage_name'
    )

    with pytest.raises(ValueError):
        get_storage_name_from_external_location('abfss://container.storage.dfs.core.windows.net/', external_location)
