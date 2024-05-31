import os
import time
from typing import Union

import attrs
from azure.core.credentials import TokenCredential
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import AzureCliCredential, DefaultAzureCredential

from dbxio.utils.databricks import ClusterType
from dbxio.utils.env import (
    AIRFLOW_UNIQUE_NAME,
    DATABRICKS_ACCESS_TOKEN,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_SERVER_HOSTNAME,
    DBX_FORCE_LOCAL,
)
from dbxio.utils.logging import get_logger

AZ_DBX_SCOPE = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default'
DBX_DEFAULT_HOST = 'databricks_default_host'
DBX_DEFAULT_SQL_HTTP_PATH = 'databricks_default_sql_http_path'
DBX_DEFAULT_WAREHOUSE_SQL_HTTP_PATH = 'databricks_default_warehouse_sql_http_path'

AZ_CRED_PROVIDER_TYPE = Union[TokenCredential, DefaultAzureCredential, AzureCliCredential]

logger = get_logger()


@attrs.define(slots=True)
class ClusterCredentials:
    """
    Credentials for Databricks cluster.
    Is fully configured if all fields are set.

    Access token can be PAT or short-lived token.
    """

    access_token: str = attrs.field(validator=attrs.validators.instance_of(str))
    server_hostname: str = attrs.field(validator=attrs.validators.instance_of(str))
    http_path: str = attrs.field(validator=attrs.validators.instance_of(str))


@attrs.frozen(slots=True)
class SemiConfiguredClusterCredentials:
    """
    Credentials for Databricks cluster without access token.
    In this case, access token will be generated from a credential provider.
    """

    http_path: str = attrs.field(validator=attrs.validators.instance_of(str))
    server_hostname: str = attrs.field(validator=attrs.validators.instance_of(str))


def check_is_airflow_installed() -> bool:
    try:
        import airflow.exceptions  # noqa
        import airflow.models  # noqa
        from airflow.providers.databricks.hooks.databricks import DatabricksHook  # noqa
    except ModuleNotFoundError:
        return False
    return True


def _get_token_airflow(retries: int = 5) -> Union[str, None]:
    from airflow.exceptions import AirflowNotFoundException
    from airflow.models import Connection as AirflowConnection
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    for retry_i in range(retries):
        try:
            c = AirflowConnection.get_connection_from_secrets(DatabricksHook.default_conn_name)
            if dbx_token := c.get_password():
                return dbx_token
        except ModuleNotFoundError:
            logger.warning('Databricks provider is not installed, not retryable error')
            break
        except AirflowNotFoundException as e:
            logger.warning(f'Could not find databricks default connection: {e}')
            time.sleep(5 * 2**retry_i)

    return None


def _get_token_az(az_cred_provider: AZ_CRED_PROVIDER_TYPE = None, retries: int = 5) -> Union[str, None]:
    """
    Get Databricks short-lived token from Azure credential provider.
    """
    az_cred_provider = az_cred_provider or DefaultAzureCredential()
    for retry_i in range(retries):
        try:
            return az_cred_provider.get_token(AZ_DBX_SCOPE).token
        except ClientAuthenticationError as e:
            logger.warning(f'Could not get Databricks token due to connection error to Azure Vault: {e}')
            time.sleep(5 * 2**retry_i)
    return None


def get_token(az_cred_provider: AZ_CRED_PROVIDER_TYPE = None, retries: int = 5) -> str:
    """
    Get token from airflow environment or Azure credential provider.
    """
    assert retries >= 1, 'Retries must be a positive integer'

    force_local = os.getenv(DBX_FORCE_LOCAL)
    if not force_local and check_is_airflow_installed():
        if token_from_airflow := _get_token_airflow():
            return token_from_airflow

    if token_from_az := _get_token_az(az_cred_provider):
        return token_from_az

    raise RuntimeError('Could not get Databricks token')


def _get_airflow_http_path(cluster_type: ClusterType) -> str:
    """
    Get default HTTP path for Databricks cluster from Airflow variables.
    """
    from airflow.models import Variable

    airflow_unique_name = os.getenv(AIRFLOW_UNIQUE_NAME)

    if cluster_type == ClusterType.ALL_PURPOSE:
        return Variable.get(f'{airflow_unique_name}-{DBX_DEFAULT_SQL_HTTP_PATH}')
    elif cluster_type == ClusterType.SQL_WAREHOUSE:
        return Variable.get(f'{airflow_unique_name}-{DBX_DEFAULT_WAREHOUSE_SQL_HTTP_PATH}')
    else:
        raise ValueError(f'Unknown cluster type: {cluster_type}')


def _get_airflow_server_hostname() -> str:
    """
    Get default server hostname for Databricks cluster from Airflow variables.
    """
    from airflow.models import Variable

    airflow_unique_name = os.getenv(AIRFLOW_UNIQUE_NAME)
    return Variable.get(f'{airflow_unique_name}-{DBX_DEFAULT_HOST}')


def get_airflow_variables(
    cluster_type: ClusterType,
    semi_configured_credentials: SemiConfiguredClusterCredentials = None,
) -> ClusterCredentials:
    """
    Get credentials for Databricks cluster from Airflow variables.
    """
    http_path = (
        semi_configured_credentials.http_path
        if semi_configured_credentials is not None
        else _get_airflow_http_path(cluster_type)
    )
    server_hostname = (
        semi_configured_credentials.server_hostname
        if semi_configured_credentials is not None
        else _get_airflow_server_hostname()
    )

    return ClusterCredentials(
        access_token=get_token(),
        server_hostname=server_hostname,
        http_path=http_path,
    )


def get_local_variables(
    az_cred_provider: AZ_CRED_PROVIDER_TYPE = None,
    semi_configured_credentials: SemiConfiguredClusterCredentials = None,
) -> ClusterCredentials:
    """
    Get credentials for Databricks cluster from local environment variables.
    Access token can be generated by Azure credential provider.
    """
    access_token = os.getenv(DATABRICKS_ACCESS_TOKEN)
    if not access_token:
        logger.debug('No DATABRICKS_ACCESS_TOKEN found, trying to get token from Azure')
        access_token = get_token(az_cred_provider)
    http_path = (
        semi_configured_credentials.http_path
        if semi_configured_credentials is not None
        else os.getenv(DATABRICKS_HTTP_PATH)
    )
    if not http_path:
        raise ValueError('DATABRICKS_HTTP_PATH is not set')
    server_hostname = (
        semi_configured_credentials.server_hostname
        if semi_configured_credentials is not None
        else os.getenv(DATABRICKS_SERVER_HOSTNAME)
    )
    if not server_hostname:
        raise ValueError('DATABRICKS_SERVER_HOSTNAME is not set')

    return ClusterCredentials(
        access_token=access_token,
        server_hostname=server_hostname,
        http_path=http_path,
    )
