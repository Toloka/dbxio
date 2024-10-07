import logging
from typing import TYPE_CHECKING

from azure.core.exceptions import AzureError
from databricks.sdk.errors.platform import PermissionDenied
from tenacity import RetryCallState, Retrying, after_log, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.exceptions import ReadTimeoutError

from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core.settings import RetryConfig

logger = get_logger()

BASE_EXCEPTIONS_TO_RETRY = (PermissionDenied, ReadTimeoutError, AzureError)


def _clear_client_cache(call_state: RetryCallState) -> None:
    """
    Gets all argument of the function from retry state, finds the client and clears its cache.
    """
    if call_state.attempt_number == 1:
        # Do not clear cache on the first attempt
        return

    from dbxio.core.client import DbxIOClient

    for arg in call_state.args:
        if isinstance(arg, DbxIOClient):
            arg.clear_cache()
            return
    for arg in call_state.kwargs.values():
        if isinstance(arg, DbxIOClient):
            arg.clear_cache()
            return


def build_retrying(settings: 'RetryConfig') -> Retrying:
    return Retrying(
        stop=stop_after_attempt(settings.max_attempts),
        wait=wait_exponential(multiplier=settings.exponential_backoff_multiplier),
        retry=retry_if_exception_type(BASE_EXCEPTIONS_TO_RETRY + settings.extra_exceptions_to_retry),
        before=_clear_client_cache,
        after=after_log(logger, log_level=logging.INFO),
    )
