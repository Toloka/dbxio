import unittest
from textwrap import dedent
from unittest.mock import patch

from dbxio.core import DbxIOClient
from dbxio.delta import types
from dbxio.delta.table import Table, TableFormat
from dbxio.delta.table_commands import (
    copy_into_table,
    create_table,
    drop_table,
    exists_table,
    merge_table,
    read_table,
    save_table_to_files,
    write_table,
)
from dbxio.delta.table_schema import TableSchema
from dbxio.utils.databricks import ClusterType
from tests.mocks.azure import MockDefaultAzureCredential


def flatten_query(query):
    return ' '.join([s.strip() for s in query.splitlines()])


def sql_mock(sql):
    """
    Returns context manager that returns one test record on __enter__ call
    """

    class MockSql:
        def __enter__(self):
            return [{'a': 1, 'b': 2}]

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def wait(self):
            pass

    return MockSql()


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
        assert observed_query == 'DROP TABLE `catalog`.`schema`.`table__dbxio_tmp`'
