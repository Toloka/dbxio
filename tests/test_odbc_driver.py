import math
import tempfile
from unittest.mock import patch

import pyarrow.parquet as pq
from deepdiff import DeepDiff
from pytest import fixture

from dbxio.core.auth import ClusterCredentials
from dbxio.sql.results import ODBC_BATCH_SIZE_TO_FETCH
from dbxio.sql.sql_driver import ODBCDriver
from dbxio.utils.databricks import ClusterType
from tests.mocks.odbc import MOCK_ROW, TOTAL_MOCK_RECORDS, mock_dbx_connect


@fixture
def cluster_credentials():
    return ClusterCredentials(
        access_token='dapi123',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


def test_odbc_driver_as_dict_wo_session_configuration(cluster_credentials, default_retrying):
    driver = ODBCDriver(
        cluster_type=ClusterType.ALL_PURPOSE,
        cluster_credentials=cluster_credentials,
        session_configuration=None,
        retrying=default_retrying,
    )
    assert driver.as_dict() == {
        'server_hostname': 'adb-123456789.10.azuredatabricks.net',
        'http_path': 'sql/protocolv1/o/111111/1-1-a',
        'access_token': 'dapi123',
        'session_configuration': None,
    }


def test_odbc_driver_as_dict_with_session_configuration(cluster_credentials, default_retrying):
    driver = ODBCDriver(
        cluster_type=ClusterType.ALL_PURPOSE,
        cluster_credentials=cluster_credentials,
        session_configuration={'conf_key': 'conf_value'},
        retrying=default_retrying,
    )
    assert driver.as_dict() == {
        'server_hostname': 'adb-123456789.10.azuredatabricks.net',
        'http_path': 'sql/protocolv1/o/111111/1-1-a',
        'access_token': 'dapi123',
        'session_configuration': {'conf_key': 'conf_value'},
    }


def test_odbc_driver_sql(cluster_credentials, default_retrying):
    with patch('databricks.sql.connect', side_effect=mock_dbx_connect):
        driver = ODBCDriver(
            cluster_type=ClusterType.ALL_PURPOSE,
            cluster_credentials=cluster_credentials,
            session_configuration=None,
            retrying=default_retrying,
        )
        data = list(driver.sql('select * from table'))
        assert all([not DeepDiff(row, MOCK_ROW.asDict()) for row in data])


def test_odbc_driver_sql_to_files(cluster_credentials, default_retrying):
    driver = ODBCDriver(
        cluster_type=ClusterType.ALL_PURPOSE,
        cluster_credentials=cluster_credentials,
        session_configuration=None,
        retrying=default_retrying,
    )
    with tempfile.TemporaryDirectory() as temp_dir, patch('databricks.sql.connect', side_effect=mock_dbx_connect):
        path_to_files = driver.sql_to_files('select * from table', results_path=temp_dir)
        files = list(path_to_files.glob('*.parquet'))
        assert len(files) == math.ceil(TOTAL_MOCK_RECORDS / ODBC_BATCH_SIZE_TO_FETCH)
        assert pq.read_table(files[0]).num_rows == min(ODBC_BATCH_SIZE_TO_FETCH, TOTAL_MOCK_RECORDS)
