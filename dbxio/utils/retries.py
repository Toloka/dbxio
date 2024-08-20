from databricks.sdk.errors.platform import PermissionDenied
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential


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


dbxio_retry = retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1),
    retry=retry_if_exception_type((PermissionDenied,)),
    reraise=True,
    before=_clear_client_cache,
)
