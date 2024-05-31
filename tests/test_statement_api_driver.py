import tempfile
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sdk.service.sql import ExecuteStatementResponse, ExternalLink, ResultData, ResultManifest
from pytest import fixture

from dbxio.core.auth import ClusterCredentials
from dbxio.core.client import DbxIOClient
from dbxio.sql.sql_driver import StatementAPIDriver
from dbxio.utils.databricks import ClusterType

MOCK_STATEMENT_ID = '12345-6789-0000'
MOCK_TOTAL_CHUNK_COUNT = 10


@fixture
def statement_api():
    return DbxIOClient.from_cluster_settings(
        http_path='sql/protocolv1/o/111111/1-1-a',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        cluster_type=ClusterType.ALL_PURPOSE,
    ).statement_api


@fixture
def cluster_credentials():
    return ClusterCredentials(
        access_token='dapi123',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


def mock_execute_statement(*args, **kwargs):
    return ExecuteStatementResponse(
        statement_id=MOCK_STATEMENT_ID,
        manifest=ResultManifest(total_chunk_count=MOCK_TOTAL_CHUNK_COUNT),
    )


def mock_get_statement_result_chunk_n(*args, **kwargs):
    return ResultData(external_links=[ExternalLink(external_link='https://adb_external_link_to_results')])


@patch(
    'databricks.sdk.service.sql.StatementExecutionAPI.execute_statement',
    side_effect=mock_execute_statement,
)
@patch(
    'databricks.sdk.service.sql.StatementExecutionAPI.get_statement_result_chunk_n',
    side_effect=mock_get_statement_result_chunk_n,
)
def test_sapi_driver_sql(mock1, mock2, statement_api, cluster_credentials, requests_mock):
    driver = StatementAPIDriver(
        cluster_type=ClusterType.ALL_PURPOSE,
        cluster_credentials=cluster_credentials,
        statement_api=statement_api,
    )

    with open('tests/resources/arrow_stream_1_plus_1.arrow', 'rb') as f:
        requests_mock.get(
            'https://adb_external_link_to_results',
            content=f.read(),
        )
    with patch('dbxio.sql.results._FutureStatementApiResult.wait', return_value=None):
        data = list(driver.sql('select * from table'))
        assert data == [{'a': 2} for _ in range(MOCK_TOTAL_CHUNK_COUNT)]


@patch(
    'databricks.sdk.service.sql.StatementExecutionAPI.execute_statement',
    side_effect=mock_execute_statement,
)
@patch(
    'databricks.sdk.service.sql.StatementExecutionAPI.get_statement_result_chunk_n',
    side_effect=mock_get_statement_result_chunk_n,
)
def test_sapi_driver_sql_to_files(mock1, mock2, statement_api, cluster_credentials, requests_mock):
    driver = StatementAPIDriver(
        cluster_type=ClusterType.ALL_PURPOSE,
        cluster_credentials=cluster_credentials,
        statement_api=statement_api,
    )
    with open('tests/resources/arrow_stream_1_plus_1.arrow', 'rb') as f:
        arrow_stream = f.read()
    requests_mock.get(
        'https://adb_external_link_to_results',
        content=arrow_stream,
    )
    with tempfile.TemporaryDirectory() as temp_dir, patch(
        'dbxio.sql.results._FutureStatementApiResult.wait', return_value=None
    ):
        path_to_files = driver.sql_to_files('select * from table', results_path=temp_dir)
        assert len(list(path_to_files.iterdir())) == MOCK_TOTAL_CHUNK_COUNT
        table = pq.read_table(f'{path_to_files}/0.parquet')
        assert pa.ipc.open_stream(arrow_stream).read_all() == table
