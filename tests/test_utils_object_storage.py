import pytest

from dbxio.utils.object_storage import ObjectStorageClient
from dbxio.utils.object_storage._aws import _S3StorageClientImpl
from dbxio.utils.object_storage._azure import _AzureBlobStorageClientImpl
from dbxio.utils.object_storage._gcp import _GCStorageClientImpl


def test_object_storage_azure_from_url():
    sl = ObjectStorageClient.from_url('abfss://container@storageaccount.dfs.core.windows.net/depts/hr/employees')
    assert isinstance(sl, _AzureBlobStorageClientImpl)
    assert sl.container_name == 'container'
    assert sl.storage_name == 'storageaccount'
    assert sl.domain_name == 'dfs.core.windows.net'
    assert sl.blobs_path == 'depts/hr/employees'

    assert sl.to_url() == 'abfss://container@storageaccount.dfs.core.windows.net/depts/hr/employees'


def test_object_storage_azure_from_storage_options():
    sl = ObjectStorageClient.from_storage_options(
        container_name='container',
        storage_name='storageaccount',
        domain_name='dfs.core.windows.net',
        blobs_path='depts/hr/employees',
        scheme='abfss',
    )
    assert isinstance(sl, _AzureBlobStorageClientImpl)
    assert sl.container_name == 'container'
    assert sl.storage_name == 'storageaccount'
    assert sl.domain_name == 'dfs.core.windows.net'
    assert sl.blobs_path == 'depts/hr/employees'

    assert sl.to_url() == 'abfss://container@storageaccount.dfs.core.windows.net/depts/hr/employees'


def test_broken_object_storage_azure():
    with pytest.raises(ValueError):
        ObjectStorageClient.from_url('abfss://container/storageaccount.dfs.core.windows.net/depts/hr/employees')
    with pytest.raises(ValueError):
        ObjectStorageClient.from_url('abfss://container/depts/hr/employees')


@pytest.mark.skip(reason='S3 storage is not implemented yet')
def test_object_storage_s3_from_url():
    sl = ObjectStorageClient.from_url('s3://bucket-name/path/to/data')
    assert isinstance(sl, _S3StorageClientImpl)
    assert sl.bucket_name == 'bucket-name'
    assert sl.object_key == 'path/to/data'

    assert sl.to_url() == 's3://bucket-name/path/to/data'


@pytest.mark.skip(reason='S3 storage is not implemented yet')
def test_object_storage_s3_from_storage_options():
    sl = ObjectStorageClient.from_storage_options(
        bucket_name='bucket-name',
        object_key='path/to/data',
        scheme='s3',
    )
    assert isinstance(sl, _S3StorageClientImpl)
    assert sl.bucket_name == 'bucket-name'
    assert sl.object_key == 'path/to/data'

    assert sl.to_url() == 's3://bucket-name/path/to/data'


@pytest.mark.skip(reason='S3 storage is not implemented yet')
def test_broken_object_storage_s3():
    with pytest.raises(ValueError):
        ObjectStorageClient.from_url('s3://bucket-name')


@pytest.mark.skip(reason='GCP storage is not implemented yet')
def test_object_storage_gs_from_url():
    sl = ObjectStorageClient.from_url('gs://bucket-name/path/to/data')
    assert isinstance(sl, _GCStorageClientImpl)
    assert sl.bucket_name == 'bucket-name'
    assert sl.object_key == 'path/to/data'

    assert sl.to_url() == 'gs://bucket-name/path/to/data'


@pytest.mark.skip(reason='GCP storage is not implemented yet')
def test_object_storage_gs_from_storage_options():
    sl = ObjectStorageClient.from_storage_options(
        bucket_name='bucket-name',
        object_key='path/to/data',
        scheme='gs',
    )
    assert isinstance(sl, _GCStorageClientImpl)
    assert sl.bucket_name == 'bucket-name'
    assert sl.object_key == 'path/to/data'

    assert sl.to_url() == 'gs://bucket-name/path/to/data'


@pytest.mark.skip(reason='GCP storage is not implemented yet')
def test_broken_object_storage_gs():
    with pytest.raises(ValueError):
        ObjectStorageClient.from_url('gs://bucket-name')
