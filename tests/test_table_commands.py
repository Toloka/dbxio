import unittest
import uuid
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest.mock import patch

import pytest

from dbxio.core import DbxIOClient
from dbxio.delta.table import Table, TableFormat
from dbxio.delta.table_commands import (
    bulk_write_local_files,
    bulk_write_table,
    copy_into_table,
    create_table,
    drop_table,
    exists_table,
    get_comment_on_table,
    get_tags_on_table,
    merge_table,
    read_table,
    save_table_to_files,
    set_comment_on_table,
    set_tags_on_table,
    unset_comment_on_table,
    unset_tags_on_table,
    write_table,
)
from dbxio.delta.table_schema import TableSchema
from dbxio.sql import types
from dbxio.utils.databricks import ClusterType
from tests.mocks.azure import MockBlobLeaseClient, MockBlobServiceClient, MockDefaultAzureCredential
from tests.mocks.sql import flatten_query, sql_mock


class TestTableCommands(unittest.TestCase):
    def setUp(self):
        self.client = DbxIOClient.from_cluster_settings(
            http_path='test_sql_endpoint_path',
            server_hostname='test_host_name',
            cluster_type=ClusterType.SQL_WAREHOUSE,
            az_cred_provider=MockDefaultAzureCredential(),
        )

        self.table = Table(
            'catalog.schema.table',
            schema=TableSchema(
                [
                    {'name': 'a', 'type': types.IntType()},
                    {'name': 'b', 'type': types.IntType()},
                ]
            ),
        )

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_exists_table(self, mock_sql):
        exists_table(self.table, self.client)
        mock_sql.assert_called_once_with('SELECT * FROM `catalog`.`schema`.`table` LIMIT 1')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_create_table(self, mock_sql):
        create_table(self.table, self.client)
        mock_sql.assert_called_once_with('CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`table` (`a` INT,`b` INT)')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_create_table_with_location(self, mock_sql):
        self.table.attributes.location = 's3://test-bucket/test-path'
        create_table(self.table, self.client)
        mock_sql.assert_called_once_with(
            'CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`table` (`a` INT,`b` INT) '
            "USING DELTA LOCATION 's3://test-bucket/test-path'"
        )

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_create_table_with_partition_by(self, mock_sql):
        self.table.attributes.partitioned_by = ['a']
        create_table(self.table, self.client)
        mock_sql.assert_called_once_with(
            'CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`table` (`a` INT,`b` INT) PARTITIONED BY (a)'
        )

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_drop_table(self, mock_sql):
        drop_table(self.table, self.client)
        mock_sql.assert_called_once_with('DROP TABLE `catalog`.`schema`.`table`')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_drop_table_force(self, mock_sql):
        drop_table(self.table, self.client, force=True)
        mock_sql.assert_called_once_with('DROP TABLE IF EXISTS `catalog`.`schema`.`table`')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_read_table(self, mock_sql):
        next(read_table(table=self.table, client=self.client))
        mock_sql.assert_called_once_with('SELECT * FROM `catalog`.`schema`.`table`')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_read_table_with_columns_subset(self, mock_sql):
        next(read_table(table=self.table, client=self.client, columns_subset=['a']))
        mock_sql.assert_called_once_with('SELECT `a` FROM `catalog`.`schema`.`table`')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_read_table_with_distinct(self, mock_sql):
        next(read_table(table=self.table, client=self.client, distinct=True))
        mock_sql.assert_called_once_with('SELECT DISTINCT * FROM `catalog`.`schema`.`table`')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_read_table_with_limit(self, mock_sql):
        next(read_table(table=self.table, client=self.client, limit_records=10))
        mock_sql.assert_called_once_with('SELECT * FROM `catalog`.`schema`.`table` LIMIT 10')

    @patch.object(DbxIOClient, 'sql_to_files', side_effect=lambda *args, **kwargs: 'test_path')
    def test_save_table_to_files(self, mock_sql_to_files):
        save_table_to_files(self.table, self.client, 's3://test-bucket/test-path')
        mock_sql_to_files.assert_called_once_with(
            'select * from `catalog`.`schema`.`table`',
            results_path='s3://test-bucket/test-path',
            max_concurrency=1,
        )

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_write_table(self, mock_sql):
        write_table(self.table, [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], self.client)
        assert mock_sql.call_args == (('INSERT INTO `catalog`.`schema`.`table` (a,b) VALUES ( 1,2 ),( 3,4 )',), {})

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_write_table_without_append(self, mock_sql):
        write_table(self.table, [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], self.client, append=False)
        assert mock_sql.call_args == (('INSERT OVERWRITE `catalog`.`schema`.`table` (a,b) VALUES ( 1,2 ),( 3,4 )',), {})

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_copy_into_table(self, mock_sql):
        copy_into_table(
            self.client,
            table=self.table,
            blob_path='test-bucket/test-path',
            table_format=TableFormat.PARQUET,
            abs_name='test_abs_name',
            abs_container_name='test_abs_container_name',
        )
        expected_query = dedent("""
        COPY INTO `catalog`.`schema`.`table`
        FROM "abfss://test_abs_container_name@test_abs_name.dfs.core.windows.net/test-bucket/test-path"
        FILEFORMAT = PARQUET

        FORMAT_OPTIONS ("mergeSchema" = "true")
        COPY_OPTIONS ("mergeSchema" = "true")
        """)
        observed_query = mock_sql.call_args[0][0].query

        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_merge_table(self, mock_sql):
        merge_table(
            table=self.table,
            new_records=[{'a': 1, 'b': 2}, {'a': 3, 'b': 4}],
            partition_by=['a', 'b'],
            client=self.client,
        )

        # check that it tries to create table
        observed_query = mock_sql.call_args_list[0][0][0]
        assert observed_query == 'CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`table__dbxio_tmp` (`a` INT,`b` INT)'

        # check that it tries to insert data into temp table
        observed_query = mock_sql.call_args_list[1][0][0]
        assert observed_query == 'INSERT OVERWRITE `catalog`.`schema`.`table__dbxio_tmp` (a,b) VALUES ( 1,2 ),( 3,4 )'

        expected_query = dedent("""
        MERGE INTO `catalog`.`schema`.`table` AS DBXIO_DESTINATION
        USING `catalog`.`schema`.`table__dbxio_tmp` AS DBXIO_SOURCE
        ON DBXIO_SOURCE.a == DBXIO_DESTINATION.a
        AND DBXIO_SOURCE.b == DBXIO_DESTINATION.b

        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

        observed_query = mock_sql.call_args_list[2][0][0]

        assert flatten_query(observed_query) == flatten_query(expected_query)

        # check that it tries to drop temp table
        observed_query = mock_sql.call_args_list[3][0][0]
        assert observed_query == 'DROP TABLE IF EXISTS `catalog`.`schema`.`table__dbxio_tmp`'

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_comment_on_table(self, mock_sql):
        set_comment_on_table(self.table, 'test_comment', self.client)
        mock_sql.assert_called_once_with('''COMMENT ON TABLE `catalog`.`schema`.`table` IS "test_comment"''')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_comment_null_on_table(self, mock_sql):
        set_comment_on_table(self.table, None, self.client)
        mock_sql.assert_called_once_with('COMMENT ON TABLE `catalog`.`schema`.`table` IS NULL')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_unset_comment_on_table(self, mock_sql):
        unset_comment_on_table(self.table, self.client)
        mock_sql.assert_called_once_with('COMMENT ON TABLE `catalog`.`schema`.`table` IS NULL')

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_get_comment_on_table(self, mock_sql):
        get_comment_on_table(self.table, self.client)
        expected_query = dedent("""
        select comment
        from system.information_schema.tables
        where
            table_catalog = 'catalog'
            and table_schema = 'schema'
            and table_name = 'table'
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_tags_on_table(self, mock_sql):
        set_tags_on_table(self.table, {'tag1': 'value1', 'tag2': 'value2'}, self.client)
        expected_query = dedent("""
        ALTER TABLE `catalog`.`schema`.`table`
        SET TAGS ("tag1" = "value1", "tag2" = "value2")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_set_tags_on_table_non_stings(self, mock_sql):
        set_tags_on_table(self.table, {'tag1': 1, 'tag2': 2, 1: [1, 2, 3]}, self.client)
        expected_query = dedent("""
        ALTER TABLE `catalog`.`schema`.`table`
        SET TAGS ("tag1" = "1", "tag2" = "2", "1" = "[1, 2, 3]")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    def test_set_tags_on_table_empty_dict(self):
        with pytest.raises(AssertionError):
            set_tags_on_table(self.table, {}, self.client)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_unset_tags_on_table(self, mock_sql):
        unset_tags_on_table(self.table, ['tag1', 'tag2'], self.client)
        expected_query = dedent("""
        ALTER TABLE `catalog`.`schema`.`table`
        UNSET TAGS ("tag1", "tag2")
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    def test_unset_tags_on_table_empty_list(self):
        with pytest.raises(AssertionError):
            unset_tags_on_table(self.table, [], self.client)

    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    def test_get_tags_on_table(self, mock_sql):
        get_tags_on_table(self.table, self.client)
        expected_query = dedent("""
        select tag_name, tag_value
        from system.information_schema.table_tags
        where
            catalog_name = 'catalog'
            and schema_name = 'schema'
            and table_name = 'table'
        """)
        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch('dbxio.core.cloud.azure.object_storage.BlobServiceClient', side_effect=MockBlobServiceClient)
    @patch('dbxio.core.cloud.azure.object_storage.BlobLeaseClient', side_effect=MockBlobLeaseClient)
    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    @patch.object(uuid, 'uuid4', side_effect=lambda: 'test_uuid')
    def test_bulk_write_table(self, mock_uuid, mock_sql, mock_blob_lease_client, mock_blob_service_client):
        bulk_write_table(
            table=self.table,
            new_records=[{'a': 1, 'b': 2}, {'a': 3, 'b': 4}],
            client=self.client,
            abs_name='test_abs_name',
            abs_container_name='test_abs_container_name',
        )
        expected_query = dedent("""
        COPY INTO `catalog`.`schema`.`table`
        FROM "abfss://test_abs_container_name@test_abs_name.dfs.core.windows.net/catalog_schema_table__dbxio_tmp__test_uuid.parquet"
        FILEFORMAT = PARQUET

        FORMAT_OPTIONS ("mergeSchema" = "true")
        COPY_OPTIONS ("mergeSchema" = "true")
        """)

        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch('dbxio.core.cloud.azure.object_storage.BlobServiceClient', side_effect=MockBlobServiceClient)
    @patch('dbxio.core.cloud.azure.object_storage.BlobLeaseClient', side_effect=MockBlobLeaseClient)
    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    @patch.object(uuid, 'uuid4', side_effect=lambda: 'test_uuid')
    @patch.dict('os.environ', {'USER': ''})
    def test_bulk_write_local_files(
        self,
        mock_uuid,
        mock_sql,
        mock_blob_lease_client,
        mock_blob_service_client,
    ):
        with TemporaryDirectory() as tmp_dir:
            for i in range(10):
                with open(f'{tmp_dir}/test_file_{i}.parquet', 'w') as f:
                    f.write('test_data')
            bulk_write_local_files(
                table=self.table,
                path=tmp_dir,
                table_format=TableFormat.PARQUET,
                client=self.client,
                abs_name='test_abs_name',
                abs_container_name='test_abs_container_name',
            )
        expected_query = dedent("""
        COPY INTO `catalog`.`schema`.`table`
        FROM "abfss://test_abs_container_name@test_abs_name.dfs.core.windows.net/test_uuid"
        FILEFORMAT = PARQUET
        PATTERN = '*.parquet'
        FORMAT_OPTIONS ("mergeSchema" = "true")
        COPY_OPTIONS ("mergeSchema" = "true")
        """)

        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)

    @patch('dbxio.core.cloud.azure.object_storage.BlobServiceClient', side_effect=MockBlobServiceClient)
    @patch('dbxio.core.cloud.azure.object_storage.BlobLeaseClient', side_effect=MockBlobLeaseClient)
    @patch.object(DbxIOClient, 'sql', side_effect=sql_mock)
    @patch.object(uuid, 'uuid4', side_effect=lambda: 'test_uuid')
    @patch.dict('os.environ', {'USER': ''})
    def test_bulk_write_local_files_one_parquet_file(
        self,
        mock_uuid,
        mock_sql,
        mock_blob_lease_client,
        mock_blob_service_client,
    ):
        with TemporaryDirectory() as tmp_dir:
            with open(f'{tmp_dir}/test_file.parquet', 'w') as f:
                f.write('test_data')
            bulk_write_local_files(
                table=self.table,
                path=tmp_dir,
                table_format=TableFormat.PARQUET,
                client=self.client,
                abs_name='test_abs_name',
                abs_container_name='test_abs_container_name',
            )
        expected_query = dedent("""
        COPY INTO `catalog`.`schema`.`table`
        FROM "abfss://test_abs_container_name@test_abs_name.dfs.core.windows.net/test_uuid/test_file.parquet"
        FILEFORMAT = PARQUET

        FORMAT_OPTIONS ("mergeSchema" = "true")
        COPY_OPTIONS ("mergeSchema" = "true")
        """)

        observed_query = mock_sql.call_args[0][0]
        assert flatten_query(observed_query) == flatten_query(expected_query)
