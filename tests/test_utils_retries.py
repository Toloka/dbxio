from databricks.sdk.errors.platform import PermissionDenied
from tenacity import stop_after_attempt, wait_fixed

from dbxio import ClusterType, DbxIOClient, dbxio_retry
from tests.mocks.azure import MockDefaultAzureCredential

client = DbxIOClient.from_cluster_settings(
    http_path='test_sql_endpoint_path',
    server_hostname='test_host_name',
    cluster_type=ClusterType.SQL_WAREHOUSE,
    az_cred_provider=MockDefaultAzureCredential(),
)

N_RETRIES = 2


class UnknownException(Exception):
    pass


@dbxio_retry
def _some_function(arg1, arg2, client: DbxIOClient, kwarg1=None, kwarg2=None):
    raise PermissionDenied('Permission denied')


@dbxio_retry
def _some_function_with_unknown_exception(arg1, arg2, client: DbxIOClient, kwarg1=None, kwarg2=None):
    raise UnknownException('Unknown exception')


def test_dbxio_retry():
    func = _some_function.retry_with(
        stop=stop_after_attempt(N_RETRIES),
        wait=wait_fixed(0),
    )
    try:
        func(1, 2, client, kwarg1=3, kwarg2=4)
    except PermissionDenied:
        pass

    # how to get stat: https://github.com/jd/tenacity/issues/486#issuecomment-2229210530
    attempt_number = func.retry.statistics.get('attempt_number') or func.statistics.get('attempt_number')

    assert attempt_number == N_RETRIES


def test_dbxio_retry_unknown_exception():
    func = _some_function_with_unknown_exception.retry_with(
        stop=stop_after_attempt(N_RETRIES * 100),
        wait=wait_fixed(0),
    )
    try:
        func(1, 2, client, kwarg1=3, kwarg2=4)
    except UnknownException:
        pass

    attempt_number = func.retry.statistics.get('attempt_number') or func.statistics.get('attempt_number')

    assert attempt_number == 1
