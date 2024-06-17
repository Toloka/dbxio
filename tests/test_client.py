import os
from unittest.mock import patch

from dbxio import ClusterType
from dbxio.core.client import DbxIOClient, DefaultDbxIOClient, DefaultSqlDbxIOClient
from dbxio.core.credentials import (
    BareAuthProvider,
    ClusterCredentials,
    ClusterEnvAuthProvider,
    DefaultCredentialProvider,
)
from dbxio.utils.env import DATABRICKS_HTTP_PATH, DATABRICKS_SERVER_HOSTNAME
from tests.mocks.azure import MockDefaultAzureCredential


def test_client_from_cluster_settings():
    client = DbxIOClient.from_cluster_settings(
        http_path='test_sql_endpoint_path',
        server_hostname='test_host_name',
        cluster_type=ClusterType.SQL_WAREHOUSE,
        az_cred_provider=MockDefaultAzureCredential(),
    )
    assert isinstance(client.credential_provider, DefaultCredentialProvider)
    assert client.credential_provider.semi_configured_credentials.http_path == 'test_sql_endpoint_path'
    assert client.credential_provider.semi_configured_credentials.server_hostname == 'test_host_name'
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname='test_host_name',
        http_path='test_sql_endpoint_path',
    )


def test_client_from_bare_provider():
    client = DbxIOClient.from_auth_provider(
        auth_provider=BareAuthProvider(
            access_token='test_access_token',
            server_hostname='test_host_name',
            http_path='test_sql_endpoint_path',
            cluster_type=ClusterType.SQL_WAREHOUSE,
        ),
    )
    assert isinstance(client.credential_provider, BareAuthProvider)
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='test_access_token',
        server_hostname='test_host_name',
        http_path='test_sql_endpoint_path',
    )


def test_client_from_env_provider():
    client = DbxIOClient.from_auth_provider(
        auth_provider=ClusterEnvAuthProvider.from_semi_configured_credentials(
            http_path='test_sql_endpoint_path',
            server_hostname='test_host_name',
            cluster_type=ClusterType.SQL_WAREHOUSE,
            az_cred_provider=MockDefaultAzureCredential(),
        ),
    )

    assert isinstance(client.credential_provider, ClusterEnvAuthProvider)
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname='test_host_name',
        http_path='test_sql_endpoint_path',
    )


@patch('dbxio.core.credentials.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
def test_default_all_purpose_client(patch_default_az_cred, monkeypatch):
    os.environ[DATABRICKS_HTTP_PATH] = 'sql/protocolv1/o/111111/1-1-a'
    os.environ[DATABRICKS_SERVER_HOSTNAME] = 'adb-123456789.10.azuredatabricks.net'

    client = DefaultDbxIOClient()

    assert isinstance(client.credential_provider, DefaultCredentialProvider)
    assert client.credential_provider.semi_configured_credentials is None
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname=os.environ[DATABRICKS_SERVER_HOSTNAME],
        http_path=os.environ[DATABRICKS_HTTP_PATH],
    )


@patch('dbxio.core.credentials.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
def test_default_sql_client(patch_default_az_cred, monkeypatch):
    os.environ[DATABRICKS_HTTP_PATH] = 'sql/protocolv1/o/111111/1-1-a'
    os.environ[DATABRICKS_SERVER_HOSTNAME] = 'adb-123456789.10.azuredatabricks.net'

    client = DefaultSqlDbxIOClient()

    assert isinstance(client.credential_provider, DefaultCredentialProvider)
    assert client.credential_provider.semi_configured_credentials is None
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname=os.environ[DATABRICKS_SERVER_HOSTNAME],
        http_path=os.environ[DATABRICKS_HTTP_PATH],
    )


@patch('azure.identity.AzureCliCredential', side_effect=MockDefaultAzureCredential)
def test_client_with_explicit_credential_provider(patch_default_az_cred, monkeypatch):
    from azure.identity import AzureCliCredential

    client = DbxIOClient.from_cluster_settings(
        http_path='test_sql_endpoint_path',
        server_hostname='test_host_name',
        cluster_type=ClusterType.SQL_WAREHOUSE,
        az_cred_provider=AzureCliCredential(),
    )

    assert isinstance(client.credential_provider, DefaultCredentialProvider)
    assert client.credential_provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname='test_host_name',
        http_path='test_sql_endpoint_path',
    )
