import os
from unittest.mock import patch

import pytest

from dbxio import ClusterType
from dbxio.core.auth import get_token
from dbxio.core.credentials import (
    BareAuthProvider,
    ClusterAirflowAuthProvider,
    ClusterCredentials,
    ClusterEnvAuthProvider,
    DefaultCredentialProvider,
)
from dbxio.core.exceptions import InsufficientCredentialsError
from dbxio.utils.env import (
    DATABRICKS_ACCESS_TOKEN,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_SERVER_HOSTNAME,
)
from tests.mocks.azure import MockDefaultAzureCredential


@patch('dbxio.core.auth.os.getenv', side_effect=lambda x, default=None: os.environ.get(x, default))
@patch('dbxio.core.auth.get_token', side_effect=lambda *args, **kwargs: get_token(*args, **kwargs))
def test_dbxio_cluster_env_auth_provider_w_token(patched_get_token, pathed_getenv):
    os.environ[DATABRICKS_HTTP_PATH] = 'sql/protocolv1/o/111111/1-1-a'
    os.environ[DATABRICKS_SERVER_HOSTNAME] = 'adb-123456789.10.azuredatabricks.net'
    os.environ[DATABRICKS_ACCESS_TOKEN] = 'dapi123'

    creds = ClusterEnvAuthProvider(cluster_type=ClusterType.SQL_WAREHOUSE).get_credentials()

    assert pathed_getenv.call_count == 3
    assert patched_get_token.call_count == 0
    assert creds == ClusterCredentials(
        access_token='dapi123',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


@patch('dbxio.core.credentials.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
@patch('dbxio.core.auth.os.getenv', side_effect=lambda x, default=None: os.environ.get(x, default))
@patch('dbxio.core.auth.get_token', side_effect=lambda *args, **kwargs: get_token(*args, **kwargs))
def test_dbxio_cluster_env_auth_provider_wo_token(
    patched_get_token, pathed_getenv, patched_default_az_cred, monkeypatch
):
    os.environ[DATABRICKS_HTTP_PATH] = 'sql/protocolv1/o/111111/1-1-a'
    os.environ[DATABRICKS_SERVER_HOSTNAME] = 'adb-123456789.10.azuredatabricks.net'
    monkeypatch.delenv(DATABRICKS_ACCESS_TOKEN, raising=False)

    provider = ClusterEnvAuthProvider(
        cluster_type=ClusterType.SQL_WAREHOUSE,
        az_cred_provider=patched_default_az_cred(),
    )
    creds = provider.get_credentials()

    assert pathed_getenv.call_count == 4
    assert patched_get_token.call_count == 1
    assert creds == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


@patch('dbxio.core.auth.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
def test_dbxio_cluster_airflow_auth_provider_wo_airflow(pached_default_az_cred):
    provider = ClusterAirflowAuthProvider(cluster_type=ClusterType.SQL_WAREHOUSE)
    with pytest.raises(InsufficientCredentialsError):
        provider.get_credentials()


@patch('dbxio.core.auth.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
def test_dbxio_default_credential_provider_all_unavailable(patch_default_az_cred, monkeypatch):
    monkeypatch.delenv(DATABRICKS_HTTP_PATH, raising=False)
    monkeypatch.delenv(DATABRICKS_SERVER_HOSTNAME, raising=False)
    monkeypatch.delenv(DATABRICKS_ACCESS_TOKEN, raising=False)

    with pytest.raises(AssertionError):
        DefaultCredentialProvider(cluster_type=ClusterType.SQL_WAREHOUSE, lazy=False)


@patch('dbxio.core.credentials.DefaultAzureCredential', side_effect=MockDefaultAzureCredential)
def test_dbxio_default_credential_provider(patch_default_az_cred, monkeypatch):
    os.environ[DATABRICKS_HTTP_PATH] = 'sql/protocolv1/o/111111/1-1-a'
    os.environ[DATABRICKS_SERVER_HOSTNAME] = 'adb-123456789.10.azuredatabricks.net'
    monkeypatch.delenv(DATABRICKS_ACCESS_TOKEN, raising=False)

    provider = DefaultCredentialProvider(cluster_type=ClusterType.SQL_WAREHOUSE)

    assert provider._successful_provider is None

    provider.ensure_set_auth_provider()
    assert isinstance(provider._successful_provider, ClusterEnvAuthProvider)

    assert provider.get_credentials() == ClusterCredentials(
        access_token='azure_access_token_for_scope_2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


def test_dbxio_bare_auth_provider():
    provider = BareAuthProvider(
        access_token='dapi123',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
        cluster_type=ClusterType.SQL_WAREHOUSE,
    )
    assert provider.get_credentials() == ClusterCredentials(
        access_token='dapi123',
        server_hostname='adb-123456789.10.azuredatabricks.net',
        http_path='sql/protocolv1/o/111111/1-1-a',
    )


def test_dbxio_bare_auth_provider_invalid():
    with pytest.raises(TypeError):
        BareAuthProvider(access_token=None, server_hostname=None, http_path=None)

    with pytest.raises(TypeError):
        BareAuthProvider(access_token=123, server_hostname=123, http_path=123)
