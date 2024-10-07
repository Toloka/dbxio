import pytest
from databricks.sdk.errors.platform import PermissionDenied
from tenacity import stop_after_attempt, wait_fixed

from dbxio import ClusterType, DbxIOClient
from dbxio.utils.retries import build_retrying
from tests.mocks.azure import MockDefaultAzureCredential


@pytest.fixture
def mock_client():
    return DbxIOClient.from_cluster_settings(
        http_path='test_sql_endpoint_path',
        server_hostname='test_host_name',
        cluster_type=ClusterType.SQL_WAREHOUSE,
        az_cred_provider=MockDefaultAzureCredential(),
    )


@pytest.fixture
def retrying(mock_client):
    retrying = build_retrying(mock_client.settings.retry_config)
    retrying.stop = stop_after_attempt(N_RETRIES)
    retrying.wait = wait_fixed(0)
    retrying.reraise = True

    return retrying


N_RETRIES = 2


class UnknownException(Exception):
    pass


def _some_function(arg1, arg2, client: DbxIOClient, kwarg1=None, kwarg2=None):
    raise PermissionDenied('Permission denied')


def _some_function_with_unknown_exception(arg1, arg2, client: DbxIOClient, kwarg1=None, kwarg2=None):
    raise UnknownException('Unknown exception')


def test_dbxio_retry(mock_client, retrying):
    try:
        retrying(_some_function, 1, 2, mock_client, kwarg1=3, kwarg2=4)
    except PermissionDenied:
        pass

    attempt_number = retrying.statistics.get('attempt_number')

    assert attempt_number == N_RETRIES


def test_dbxio_retry_unknown_exception(mock_client, retrying):
    retrying.stop = stop_after_attempt(N_RETRIES)
    try:
        retrying(_some_function_with_unknown_exception, 1, 2, mock_client, kwarg1=3, kwarg2=4)
    except UnknownException:
        pass

    attempt_number = retrying.statistics.get('attempt_number')

    assert attempt_number == 1
