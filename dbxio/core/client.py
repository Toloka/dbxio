import logging
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, Optional, Union

import attrs
from azure.identity import AzureCliCredential, DefaultAzureCredential
from databricks.sdk import StatementExecutionAPI, WorkspaceClient

from dbxio.core.auth import AZ_CRED_PROVIDER_TYPE, SemiConfiguredClusterCredentials
from dbxio.core.credentials import BaseAuthProvider, DefaultCredentialProvider
from dbxio.delta.query import BaseDatabricksQuery
from dbxio.delta.sql_driver import SQLDriver, get_sql_driver
from dbxio.delta.sql_utils import _FutureBaseResult
from dbxio.utils.databricks import ClusterType


@attrs.define(slots=True)
class DbxIOClient:
    """
    Client for interacting with Databricks.

    To create a client, you can use one of the following methods:
    >>> client = DbxIOClient.from_cluster_settings(
    ...     http_path='sql/protocolv1/o/2350007385231210/abcdefg-12345',
    ...     server_hostname='adb-2350007385231210.10.azuredatabricks.net',
    ...     cluster_type=ClusterType.ALL_PURPOSE,
    ... )
    or
    >>> client = DbxIOClient.from_auth_provider(
    ...     auth_provider=DefaultCredentialProvider(
    ...         cluster_type=ClusterType.ALL_PURPOSE,
    ...         az_cred_provider=AzureCliCredential(),
    ...         http_path='sql/protocolv1/o/2350007385231210/abcdefg-12345',
    ...         server_hostname='adb-2350007385231210.10.azuredatabricks.net',
    ...     ),
    ... )
    """

    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))

    credential_provider: Union[BaseAuthProvider, DefaultCredentialProvider, None] = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            attrs.validators.instance_of((BaseAuthProvider, DefaultCredentialProvider))
        ),
    )
    semi_configured_credentials: Union[SemiConfiguredClusterCredentials, None] = attrs.field(default=None)

    session_configuration: Optional[Dict[str, Any]] = None

    az_cred_provider: AZ_CRED_PROVIDER_TYPE = attrs.Factory(DefaultAzureCredential)

    def __attrs_post_init__(self):
        if self.credential_provider is None:
            self.credential_provider = DefaultCredentialProvider(
                cluster_type=self.cluster_type,
                az_cred_provider=self.az_cred_provider,
                http_path=self.semi_configured_credentials.http_path,
                server_hostname=self.semi_configured_credentials.server_hostname,
            )

        logging.debug(f'Using {self.az_cred_provider.__class__.__name__} as the credential provider')

    @classmethod
    def from_cluster_settings(cls, http_path: str, server_hostname: str, cluster_type: ClusterType, **kwargs):
        """
        Create a client from the cluster settings. Use this method if you want to use PAT for authentication.
        """
        semi_configured_credentials = SemiConfiguredClusterCredentials(
            http_path=http_path,
            server_hostname=server_hostname,
        )
        return cls(
            semi_configured_credentials=semi_configured_credentials,
            cluster_type=cluster_type,
            **kwargs,
        )

    @classmethod
    def from_auth_provider(cls, auth_provider: Union[BaseAuthProvider, DefaultCredentialProvider], **kwargs):
        """
        Create a client from the auth provider.
        Use this method if you want to generate a short-lived token based on the available authentication method.
        """
        return cls(credential_provider=auth_provider, cluster_type=auth_provider.cluster_type, **kwargs)

    @property
    def _cluster_credentials(self):
        return self.credential_provider.get_credentials(semi_configured_credentials=self.semi_configured_credentials)

    @property
    def workspace_api(self) -> WorkspaceClient:
        return WorkspaceClient(
            host=self._cluster_credentials.server_hostname,
            token=self._cluster_credentials.access_token,
        )

    @property
    def statement_api(self) -> StatementExecutionAPI:
        return self.workspace_api.statement_execution

    @cached_property
    def _sql_driver(self) -> SQLDriver:
        return get_sql_driver(
            cluster_type=self.cluster_type,
            cluster_credentials=self._cluster_credentials,
            statement_api=self.statement_api,
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

        return self._sql_driver.sql_to_files(query, results_path, max_concurrency)


class DefaultDbxIOClient(DbxIOClient):
    """
    Default client for all-purpose clusters.

    Short and user-friendly way to create a client if all required environment variables are set.
    """

    def __init__(self, session_configuration: Optional[Dict[str, Any]] = None):
        super().__init__(
            cluster_type=ClusterType.ALL_PURPOSE,
            session_configuration=session_configuration,
        )


class DefaultSqlDbxIOClient(DbxIOClient):
    """
    Default client for SQL warehouses.

    Short and user-friendly way to create a client if all required environment variables are set.
    """

    def __init__(self, session_configuration: Optional[Dict[str, Any]] = None):
        super().__init__(
            cluster_type=ClusterType.SQL_WAREHOUSE,
            session_configuration=session_configuration,
        )


class DefaultNebiusSqlClient(DbxIOClient):
    """
    Default client for SQL warehouses with Azure CLI as the credential provider.

    On Nebius VMs, Azure CLI is the default credential provider.
    """

    def __init__(self, session_configuration: Optional[Dict[str, Any]] = None):
        super().__init__(
            cluster_type=ClusterType.SQL_WAREHOUSE,
            az_cred_provider=AzureCliCredential(),
            session_configuration=session_configuration,
        )
