import pytest
from databricks.sdk.service.catalog import VolumeType

from dbxio import Volume


def test_managed_volume():
    v = Volume(catalog='catalog', schema='schema', name='name', volume_type=VolumeType.MANAGED)

    assert v.is_managed()


def test_external_volume():
    v = Volume(
        catalog='catalog',
        schema='schema',
        name='name',
        volume_type=VolumeType.EXTERNAL,
        storage_location='location',
    )

    assert v.is_external()


def test_full_name():
    v = Volume(catalog='catalog', schema='schema', name='name')
    assert v.full_name == 'catalog.schema.name'
    assert v.safe_full_name == '`catalog`.`schema`.`name`'


def test_mount_path():
    v = Volume(catalog='catalog', schema='schema', name='name')
    assert v.mount_path == '/Volumes/catalog/schema/name'


def test_external_volume_without_storage_location():
    with pytest.raises(ValueError):
        Volume(catalog='catalog', schema='schema', name='name', volume_type=VolumeType.EXTERNAL)
