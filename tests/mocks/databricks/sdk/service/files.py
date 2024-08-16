from databricks.sdk.service.catalog import VolumeInfo, VolumeType


class MockDownloadResult:
    class contents:
        @staticmethod
        def read(n: int = -1):
            return b'test'


mock_volume_info_external = VolumeInfo(
    catalog_name='catalog',
    schema_name='schema',
    name='volume',
    volume_type=VolumeType.EXTERNAL,
    storage_location='abfss://container@storage_account.dfs.core.windows.net/dir',
)

mock_volume_info_managed = VolumeInfo(
    catalog_name='catalog',
    schema_name='schema',
    name='volume',
    volume_type=VolumeType.MANAGED,
)
