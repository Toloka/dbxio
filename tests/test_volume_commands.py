from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest.mock import patch

import pytest
from databricks.sdk.errors.platform import ResourceDoesNotExist
from databricks.sdk.service.catalog import VolumesAPI, VolumeType
from databricks.sdk.service.files import DirectoryEntry, FilesAPI

from dbxio import ClusterType
from dbxio.core.client import DbxIOClient
from dbxio.core.cloud.azure.object_storage import _AzureBlobStorageClientImpl
from dbxio.volume.volume_commands import (
    Volume,
    _download_external_volume,
    _download_managed_volume,
    _download_single_file_from_managed_volume,
    create_volume,
    download_volume,
    drop_volume,
    get_comment_on_volume,
    get_tags_on_volume,
    set_comment_on_volume,
    set_tags_on_volume,
    unset_comment_on_volume,
    unset_tags_on_volume,
)
from tests.mocks.azure import (
    MockBlob,
    MockBlobLeaseClient,
    MockBlobServiceClient,
    MockContainerClient,
    MockDefaultAzureCredential,
)
from tests.mocks.databricks.sdk.service.files import (
    MockDownloadResult,
    mock_volume_info_external,
    mock_volume_info_managed,
)
from tests.mocks.sql import flatten_query, sql_mock


@pytest.fixture
def mock_list_directory_contents_return_values():
    yield iter(
        [
            DirectoryEntry(path='path', is_directory=True, name='path'),
            DirectoryEntry(path='path/to', is_directory=True, name='to'),
            DirectoryEntry(path='path/to/file1.parquet', is_directory=False, name='file1.parquet'),
            DirectoryEntry(path='path/to/file2.parquet', is_directory=False, name='file2.parquet'),
            DirectoryEntry(path='path/to/dir', is_directory=True, name='dir'),
            DirectoryEntry(path='path/to/dir/file3.parquet', is_directory=False, name='file3.parquet'),
        ]
    )


@pytest.fixture
def mock_list_directory_contents_return_values_filtered():
    yield iter(
        [
            DirectoryEntry(path='path', is_directory=True, name='path'),
            DirectoryEntry(path='path/to', is_directory=True, name='to'),
            DirectoryEntry(path='path/to/dir', is_directory=True, name='dir'),
            DirectoryEntry(path='path/to/dir/file3.parquet', is_directory=False, name='file3.parquet'),
        ]
    )


def _mock_download_blob_tree(object_storage_client, local_path: Path, prefix_path):
    """
    File structure: path/to/blobs
    dir/file
    dir/subdir/file
    dir/subdir/subdir2/file
    """
    (local_path / 'dir' / 'subdir' / 'subdir2').mkdir(parents=True, exist_ok=True)
    (local_path / 'dir' / 'file').touch()
    (local_path / 'dir' / 'subdir' / 'file').touch()
    (local_path / 'dir' / 'subdir' / 'subdir2' / 'file').touch()
    if prefix_path == 'dir':
        return

    for path in local_path.glob('**/*'):
        relative_path = str(path.relative_to(local_path))
        if prefix_path.startswith(relative_path):
            continue
        if not relative_path.startswith(prefix_path):
            if path.is_dir():
                rmtree(path, ignore_errors=True)
            path.unlink()


volume = Volume(catalog='catalog', schema='schema', name='volume')
client = DbxIOClient.from_cluster_settings(
    http_path='test_sql_endpoint_path',
    server_hostname='test_host_name',
    cluster_type=ClusterType.SQL_WAREHOUSE,
    az_cred_provider=MockDefaultAzureCredential(),
)


@patch.object(VolumesAPI, 'create', return_value=None)
@patch('dbxio.volume.volume_commands.exists_volume', return_value=False)
def test_create_volume(mockexists_volume, mock_volume_create):
    volume = Volume(catalog='catalog', schema='schema', name='volume')
    create_volume(volume, client)
    mock_volume_create.assert_called_once_with(
        catalog_name='catalog',
        schema_name='schema',
        name='volume',
        volume_type=VolumeType.MANAGED,
        storage_location=None,
    )


@patch.object(VolumesAPI, 'create', return_value=None)
@patch('dbxio.volume.volume_commands.exists_volume', return_value=True)
def test_create_volume__volume_exists(mockexists_volume, mock_volume_create):
    volume = Volume(catalog='catalog', schema='schema', name='volume')
    create_volume(volume, client)
    mock_volume_create.assert_not_called()


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_set_comment_on_volume(mock_sql):
    set_comment_on_volume(volume, 'test_comment', client)
    mock_sql.assert_called_once_with('''COMMENT ON VOLUME `catalog`.`schema`.`volume` IS "test_comment"''')


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_set_comment_null_on_volume(mock_sql):
    set_comment_on_volume(volume, None, client)
    mock_sql.assert_called_once_with('COMMENT ON VOLUME `catalog`.`schema`.`volume` IS NULL')


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_unset_comment_on_volume(mock_sql):
    unset_comment_on_volume(volume, client)
    mock_sql.assert_called_once_with('COMMENT ON VOLUME `catalog`.`schema`.`volume` IS NULL')


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_get_comment_on_volume(mock_sql):
    get_comment_on_volume(volume, client)
    expected_query = dedent("""
    select comment
    from system.information_schema.volumes
    where
        catalog_name = 'catalog'
        and schema_name = 'schema'
        and volume_name = 'volume'
    """)
    observed_query = mock_sql.call_args[0][0]
    assert flatten_query(observed_query) == flatten_query(expected_query)


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_set_tags_on_volume(mock_sql):
    set_tags_on_volume(volume, {'tag1': 'value1', 'tag2': 'value2'}, client)
    expected_query = dedent("""
    ALTER VOLUME `catalog`.`schema`.`volume`
    SET TAGS ("tag1" = "value1", "tag2" = "value2")
    """)
    observed_query = mock_sql.call_args[0][0]
    assert flatten_query(observed_query) == flatten_query(expected_query)


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_set_tags_on_volume_non_stings(mock_sql):
    set_tags_on_volume(volume, {'tag1': 1, 'tag2': 2, 1: [1, 2, 3]}, client)
    expected_query = dedent("""
    ALTER VOLUME `catalog`.`schema`.`volume`
    SET TAGS ("tag1" = "1", "tag2" = "2", "1" = "[1, 2, 3]")
    """)
    observed_query = mock_sql.call_args[0][0]
    assert flatten_query(observed_query) == flatten_query(expected_query)


def test_set_tags_on_volume_empty_dict():
    with pytest.raises(ValueError):
        set_tags_on_volume(volume, {}, client)


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_unset_tags_on_volume(mock_sql):
    unset_tags_on_volume(volume, ['tag1', 'tag2'], client)
    expected_query = dedent("""
    ALTER VOLUME `catalog`.`schema`.`volume`
    UNSET TAGS ("tag1", "tag2")
    """)
    observed_query = mock_sql.call_args[0][0]
    assert flatten_query(observed_query) == flatten_query(expected_query)


def test_unset_tags_on_volume_empty_list():
    with pytest.raises(ValueError):
        unset_tags_on_volume(volume, [], client)


@patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
def test_get_tags_on_volume(mock_sql):
    get_tags_on_volume(volume, client)
    expected_query = dedent("""
    select tag_name, tag_value
    from system.information_schema.volume_tags
    where
        catalog_name = 'catalog'
        and schema_name = 'schema'
        and volume_name = 'volume'
    """)
    observed_query = mock_sql.call_args[0][0]
    assert flatten_query(observed_query) == flatten_query(expected_query)


@patch('dbxio.volume.volume_commands.download_blob_tree', side_effect=_mock_download_blob_tree)
def test_download_external_volume(mock_download_blob_tree):
    storage_location = 'abfss://container@storage_account.dfs.core.windows.net/dir'
    with TemporaryDirectory() as temp_dir:
        _download_external_volume(Path(temp_dir), storage_location, '')

        assert sorted(Path(temp_dir).glob('**/*')) == sorted(
            [
                Path(temp_dir) / 'dir',
                Path(temp_dir) / 'dir' / 'file',
                Path(temp_dir) / 'dir' / 'subdir',
                Path(temp_dir) / 'dir' / 'subdir' / 'file',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2' / 'file',
            ]
        )


def test_download_single_file_from_managed_volume():
    with TemporaryDirectory() as temp_dir:
        with patch.object(FilesAPI, 'download') as mock_files_download:
            mock_files_download.return_value = MockDownloadResult()

            _download_single_file_from_managed_volume(
                local_path=Path(temp_dir),
                file_path='path/to/file.parquet',
                client=client,
            )

            assert sorted(Path(temp_dir).glob('**/*')) == sorted(
                [
                    Path(temp_dir) / 'file.parquet',
                ]
            )


@patch('dbxio.volume.volume_commands._check_if_path_is_remote_file', return_value=False)
@patch.object(FilesAPI, 'download', return_value=MockDownloadResult())
def test_download_managed_volume(mock_files_download, mock_check_file, mock_list_directory_contents_return_values):
    with patch.object(
        FilesAPI,
        'list_directory_contents',
        side_effect=lambda *args, **kwargs: mock_list_directory_contents_return_values,
    ):
        with TemporaryDirectory() as temp_dir:
            _download_managed_volume(local_path=Path(temp_dir), volume=volume, client=client)

            assert sorted(Path(temp_dir).glob('**/*.parquet')) == sorted(
                [
                    Path(temp_dir) / 'path' / 'to' / 'file1.parquet',
                    Path(temp_dir) / 'path' / 'to' / 'file2.parquet',
                    Path(temp_dir) / 'path' / 'to' / 'dir' / 'file3.parquet',
                ]
            )


@patch('dbxio.volume.volume_commands.download_blob_tree', side_effect=_mock_download_blob_tree)
@patch.object(VolumesAPI, 'read', return_value=mock_volume_info_external)
def test_download_volume__external_type(mock_volume_read, mock_download_blob_tree):
    with TemporaryDirectory() as temp_dir:
        download_volume(
            path=Path(temp_dir),
            catalog_name='catalog',
            schema_name='schema',
            volume_name='volume',
            client=client,
        )

        assert sorted(Path(temp_dir).glob('**/*')) == sorted(
            [
                Path(temp_dir) / 'dir',
                Path(temp_dir) / 'dir' / 'file',
                Path(temp_dir) / 'dir' / 'subdir',
                Path(temp_dir) / 'dir' / 'subdir' / 'file',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2' / 'file',
            ]
        )


@patch('dbxio.volume.volume_commands.download_blob_tree', side_effect=_mock_download_blob_tree)
@patch.object(VolumesAPI, 'read', return_value=mock_volume_info_external)
def test_download_volume__external_type_with_volume_path(mock_volume_read, mock_download_blob_tree):
    with TemporaryDirectory() as temp_dir:
        download_volume(
            path=Path(temp_dir),
            catalog_name='catalog',
            schema_name='schema',
            volume_name='volume',
            client=client,
            volume_path='subdir',
        )

        assert sorted(Path(temp_dir).glob('**/*')) == sorted(
            [
                Path(temp_dir) / 'dir',
                Path(temp_dir) / 'dir' / 'subdir',
                Path(temp_dir) / 'dir' / 'subdir' / 'file',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2',
                Path(temp_dir) / 'dir' / 'subdir' / 'subdir2' / 'file',
            ]
        )


@patch('dbxio.volume.volume_commands._check_if_path_is_remote_file', return_value=False)
@patch.object(FilesAPI, 'download', return_value=MockDownloadResult())
@patch.object(VolumesAPI, 'read', return_value=mock_volume_info_managed)
def test_download_volume__managed_type(
    mock_volume_read, mock_files_download, mock_check_file, mock_list_directory_contents_return_values
):
    with patch.object(
        FilesAPI,
        'list_directory_contents',
        side_effect=lambda *args, **kwargs: mock_list_directory_contents_return_values,
    ):
        with TemporaryDirectory() as temp_dir:
            download_volume(
                path=Path(temp_dir),
                catalog_name='catalog',
                schema_name='schema',
                volume_name='volume',
                client=client,
            )

            assert sorted(Path(temp_dir).glob('**/*.parquet')) == sorted(
                [
                    Path(temp_dir) / 'path' / 'to' / 'file1.parquet',
                    Path(temp_dir) / 'path' / 'to' / 'file2.parquet',
                    Path(temp_dir) / 'path' / 'to' / 'dir' / 'file3.parquet',
                ]
            )


@patch('dbxio.volume.volume_commands._check_if_path_is_remote_file', return_value=False)
@patch.object(FilesAPI, 'download', return_value=MockDownloadResult())
@patch.object(VolumesAPI, 'read', return_value=mock_volume_info_managed)
def test_download_volume__managed_type_with_volume_path(
    mock_volume_read, mock_files_download, mock_check_file, mock_list_directory_contents_return_values_filtered
):
    with patch.object(
        FilesAPI,
        'list_directory_contents',
        side_effect=lambda *args, **kwargs: mock_list_directory_contents_return_values_filtered,
    ):
        with TemporaryDirectory() as temp_dir:
            download_volume(
                path=Path(temp_dir),
                catalog_name='catalog',
                schema_name='schema',
                volume_name='volume',
                client=client,
                volume_path='path/to/dir',
            )

            assert sorted(Path(temp_dir).glob('**/*.parquet')) == sorted(
                [
                    Path(temp_dir) / 'path' / 'to' / 'dir' / 'file3.parquet',
                ]
            )


@patch.object(VolumesAPI, 'delete', return_value=None)
def test_drop_volume__managed(mock_volume_delete):
    drop_volume(volume, client)
    mock_volume_delete.assert_called_once_with(volume.full_name)


@patch('dbxio.core.cloud.azure.object_storage.BlobLeaseClient', side_effect=MockBlobLeaseClient)
@patch('dbxio.core.cloud.azure.object_storage.BlobServiceClient', side_effect=MockBlobServiceClient)
@patch.object(VolumesAPI, 'delete', return_value=None)
@patch.object(
    MockContainerClient,
    'list_blobs',
    return_value=[MockBlob('path/to/file.parquet'), MockBlob('path/to/dir/file.parquet')],
)
@patch.object(_AzureBlobStorageClientImpl, 'try_delete_blob', return_value=None)
def test_drop_volume__external(
    mock_try_delete_blob,
    mock_list_blobs,
    mock_volume_delete,
    mock_blob_service_client,
    mock_blob_lease_client,
):
    volume = Volume(
        catalog='catalog',
        schema='schema',
        name='volume',
        volume_type=VolumeType.EXTERNAL,
        storage_location='abfss://container@storage_account.dfs.core.windows.net/path',
    )
    drop_volume(volume, client)
    mock_volume_delete.assert_called_once_with(volume.full_name)

    assert mock_try_delete_blob.call_count == 2


@patch.object(VolumesAPI, 'delete', side_effect=ResourceDoesNotExist())
def test_drop_volume_force(mock_volume_delete):
    volume = Volume(
        catalog='catalog',
        schema='schema',
        name='volume',
        volume_type=VolumeType.MANAGED,
    )
    drop_volume(volume, client, force=True)
