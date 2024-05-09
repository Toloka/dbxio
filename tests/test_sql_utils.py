import unittest
from unittest.mock import patch

from deepdiff import DeepDiff

from dbxio import ClusterType
from dbxio.core.client import DbxIOClient
from dbxio.core.credentials import BareAuthProvider
from tests.mocks.odbc import MOCK_ROW, MockConnection, MockCursor


def mock_dbx_connect(server_hostname, http_path, access_token, **kwargs):
    return MockConnection(server_hostname, http_path, access_token, **kwargs)


class TestSqlUtils(unittest.TestCase):
    def setUp(self):
        self.client = DbxIOClient.from_auth_provider(
            auth_provider=BareAuthProvider(
                access_token='access_test_token',
                server_hostname='test_host_name',
                http_path='test_sql_endpoint_path',
                cluster_type=ClusterType.ALL_PURPOSE,
            )
        )

    @patch('databricks.sql.connect', side_effect=mock_dbx_connect)
    @patch('databricks.sql.client.Connection', side_effect=MockConnection)
    @patch('databricks.sql.client.Cursor', side_effect=MockCursor)
    def test_run_sql_query_wo_results(self, mock_cursor, mock_conn, mock_sql_connect_func):
        result = self.client.sql('SELECT * FROM database_name.schema_name.table_name')
        self.assertIsNotNone(result)

    @patch('databricks.sql.connect', side_effect=mock_dbx_connect)
    @patch('databricks.sql.client.Connection', side_effect=MockConnection)
    @patch('databricks.sql.client.Cursor', side_effect=MockCursor)
    def test_run_sql_query_w_results(self, mock_cursor, mock_conn, mock_sql_connect_func):
        with self.client.sql('SELECT * FROM database_name.schema_name.table_name') as fq:
            result = list(fq)

        for record in result:
            assert not DeepDiff(record, MOCK_ROW.asDict())
