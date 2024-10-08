from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import attrs
from databricks.sdk import StatementExecutionAPI, WorkspaceClient

from dbxio.core.credentials import AZ_CRED_PROVIDER_TYPE, BaseAuthProvider, DefaultCredentialProvider
from dbxio.core.settings import Settings
from dbxio.sql.query import BaseDatabricksQuery
from dbxio.sql.results import _FutureBaseResult
from dbxio.sql.sql_driver import SQLDriver, get_sql_driver
from dbxio.utils.databricks import ClusterType
from dbxio.utils.logging import get_logger
from dbxio.utils.retries import build_retrying

if TYPE_CHECKING:
    from tenacity import Retrying

logger = get_logger()


@attrs.define(slots=True)
class DbxIOClient:
    """
    Client for interacting with Databricks.

    To create a client, you can use one of the following methods:
    >>> client = DbxIOClient.from_cluster_settings(
    ...     http_path='<YOUR_HTTP_PATH>',
    ...     server_hostname='<YOUR_SERVER_HOSTNAME>',
    ...     cluster_type=ClusterType.ALL_PURPOSE,
    ... )
    or
    >>> client = DbxIOClient.from_auth_provider(
    ...     auth_provider=DefaultCredentialProvider(
    ...         cluster_type=ClusterType.ALL_PURPOSE,
    ...         az_cred_provider=AzureCliCredential(),
    ...         http_path='<YOUR_HTTP_PATH>',
    ...         server_hostname='<YOUR_SERVER_HOSTNAME>',
    ...     ),
    ... )
    """

    credential_provider: Union[BaseAuthProvider, DefaultCredentialProvider] = attrs.field(
        validator=attrs.validators.instance_of((BaseAuthProvider, DefaultCredentialProvider))
    )
    settings: Settings = attrs.field(validator=attrs.validators.instance_of(Settings))

    session_configuration: Optional[Dict[str, Any]] = None

    def clear_cache(self):
        self.credential_provider.clear_cache()

    @classmethod
    def from_cluster_settings(
        cls,
        http_path: str,
        server_hostname: str,
        cluster_type: ClusterType,
        az_cred_provider: AZ_CRED_PROVIDER_TYPE = None,
        settings: Optional[Settings] = None,
        **kwargs,
    ):
        """
        Create a client from the cluster settings. Use this method if you want to use PAT for authentication.
        """
        logger.info(
            'Creating a client from the cluster settings (http-path: %s, server-hostname: %s, '
            'cluster-type: %s, az-cred-provider: %s, settings: %s)',
            http_path,
            server_hostname,
            cluster_type,
            az_cred_provider,
            settings,
        )
        provider = DefaultCredentialProvider(
            cluster_type=cluster_type,
            az_cred_provider=az_cred_provider,
            http_path=http_path,
            server_hostname=server_hostname,
        )
        return cls.from_auth_provider(auth_provider=provider, settings=settings or Settings(), **kwargs)

    @classmethod
    def from_auth_provider(
        cls,
        auth_provider: Union[BaseAuthProvider, DefaultCredentialProvider],
        settings: Optional[Settings] = None,
        **kwargs,
    ):
        """
        Create a client from the auth provider.
        Use this method if you want to generate a short-lived token based on the available authentication method.
        """
        logger.info('Creating a client from the auth provider: %s', auth_provider.__class__.__name__)
        return cls(credential_provider=auth_provider, settings=settings or Settings(), **kwargs)

    @property
    def _cluster_credentials(self):
        return self.credential_provider.get_credentials()

    @property
    def workspace_api(self) -> WorkspaceClient:
        return WorkspaceClient(
            host=self._cluster_credentials.server_hostname,
            token=self._cluster_credentials.access_token,
        )

    @property
    def statement_api(self) -> StatementExecutionAPI:
        return self.workspace_api.statement_execution

    @property
    def retrying(self) -> 'Retrying':
        return build_retrying(self.settings.retry_config)

    @property
    def _sql_driver(self) -> SQLDriver:
        return get_sql_driver(
            cluster_type=self.credential_provider.cluster_type,
            cluster_credentials=self._cluster_credentials,
            statement_api=self.statement_api,
            retrying=self.retrying,
            session_configuration=self.session_configuration,
        )

    def sql(self, query: Union[str, BaseDatabricksQuery]) -> _FutureBaseResult:
        """
        Execute the SQL query and return the results as a future.
        The future can be waited with .wait() method or iterated over.

        Results might be combined into chunks, so it's recommended to flatten them if you need a list of rows.
        """
        return self._sql_driver.sql(query)

    def sql_to_files(
        self,
        query: Union[str, BaseDatabricksQuery],
        results_path: str,
        max_concurrency: int = 1,
    ) -> Path:
        """
        Execute the SQL query and save the results to the specified directory.
        Returns the path to the directory with the results including the statement ID.
        """
        return self.retrying(self._sql_driver.sql_to_files, query, results_path, max_concurrency)


class DefaultDbxIOClient(DbxIOClient):
    """
    Default client for all-purpose clusters.

    Short and user-friendly way to create a client if all required environment variables are set.
    """

    def __init__(self, session_configuration: Optional[Dict[str, Any]] = None):
        logger.info('Creating a default client for all-purpose clusters')
        super().__init__(
            credential_provider=DefaultCredentialProvider(cluster_type=ClusterType.ALL_PURPOSE),
            session_configuration=session_configuration,
            settings=Settings(),
        )


class DefaultSqlDbxIOClient(DbxIOClient):
    """
    Default client for SQL warehouses.

    Short and user-friendly way to create a client if all required environment variables are set.
    """

    def __init__(self, session_configuration: Optional[Dict[str, Any]] = None):
        logger.info('Creating a default client for SQL warehouses')
        super().__init__(
            credential_provider=DefaultCredentialProvider(cluster_type=ClusterType.SQL_WAREHOUSE),
            session_configuration=session_configuration,
            settings=Settings(),
        )
