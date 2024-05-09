from concurrent.futures import Future
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq
from databricks.sdk.service.sql import ExecuteStatementResponse, ResultManifest, StatementState, StatementStatus
from deepdiff import DeepDiff
from pytest import fixture

from dbxio.delta.sql_utils import ODBC_BATCH_SIZE_TO_FETCH, _FutureODBCResult
from tests.mocks.odbc import MOCK_ROW, MockConnection, MockCursor


@fixture
def future():
    f = Future()
    f.set_result((MockConnection('', '', ''), MockCursor()))
    f._state = 'FINISHED'

    return f


@fixture
def future_big_result():
    f = Future()
    f.set_result((MockConnection('', '', ''), MockCursor(total_records=1000000)))
    f._state = 'FINISHED'

    return f


@fixture
def execution_response_succeeded():
    return ExecuteStatementResponse(
        manifest=ResultManifest(),
        statement_id='111',
        status=StatementStatus(state=StatementState.SUCCEEDED),
    )


@fixture
def execution_response_failed():
    return ExecuteStatementResponse(
        manifest=ResultManifest(),
        statement_id='111',
        status=StatementStatus(state=StatementState.FAILED),
    )


@fixture
def execution_response_pending():
    return ExecuteStatementResponse(
        manifest=ResultManifest(),
        statement_id='111',
        status=StatementStatus(state=StatementState.PENDING),
    )


def test_future_query_result(future):
    res = _FutureODBCResult(future)
    with res as f:
        for record in f:
            assert not DeepDiff(record, MOCK_ROW.asDict())


def test_future_query_result_save_to_files(future):
    with TemporaryDirectory() as tmpdir:
        res = _FutureODBCResult(future)
        full_results_path = res.download_and_save(results_path=tmpdir)
        table = pq.read_table(f'{full_results_path}/0.parquet')
        assert table.num_rows == MockCursor().total_records


def test_future_query_result_big_result(future_big_result):
    with TemporaryDirectory() as tmpdir:
        res = _FutureODBCResult(future_big_result)
        full_results_path = res.download_and_save(results_path=tmpdir)

        files = list(full_results_path.glob('*.parquet'))
        assert len(files) == res.cursor.total_records // ODBC_BATCH_SIZE_TO_FETCH
