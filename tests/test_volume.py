from unittest.mock import patch

from databricks.sdk.service.catalog import VolumesAPI, VolumeType

import dbxio
from tests.mocks.azure import MockDefaultAzureCredential
from tests.mocks.databricks.sdk.service.files import mock_volume_info_external

client = dbxio.DbxIOClient.from_cluster_settings(
    http_path='test_sql_endpoint_path',
    server_hostname='test_host_name',
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    az_cred_provider=MockDefaultAzureCredential(),
)


def test_managed_volume():
    v = dbxio.Volume(catalog='catalog', schema='schema', name='name', volume_type=VolumeType.MANAGED)

    assert v.is_managed()


def test_external_volume():
    v = dbxio.Volume(
        catalog='catalog',
        schema='schema',
        name='name',
        volume_type=VolumeType.EXTERNAL,
        storage_location='location',
    )

    assert v.is_external()


def test_full_name():
    v = dbxio.Volume(catalog='catalog', schema='schema', name='name')
    assert v.full_name == 'catalog.schema.name'
    assert v.safe_full_name == '`catalog`.`schema`.`name`'


def test_mount_path():
    v = dbxio.Volume(catalog='catalog', schema='schema', name='name')
    assert v.mount_path == '/Volumes/catalog/schema/name'


@patch.object(VolumesAPI, 'read', return_value=mock_volume_info_external)
def test_volume_from_url(mock_read):
    v = dbxio.Volume.from_url('/Volumes/catalog/schema/name', client)
    assert v.mount_path == '/Volumes/catalog/schema/name'
    assert v.catalog == 'catalog'
    assert v.schema == 'schema'
    assert v.name == 'name'
    assert v.volume_type == VolumeType.EXTERNAL
    assert v.storage_location == mock_volume_info_external.storage_location
