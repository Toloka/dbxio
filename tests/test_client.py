import unittest

from dbxio import ClusterType
from dbxio.core.client import DbxIOClient
from dbxio.core.credentials import BareAuthProvider, ClusterCredentials
from tests.mocks.azure import MockDefaultAzureCredential


class TestClients(unittest.TestCase):
    def setUp(self):
        self.bare_client = DbxIOClient.from_auth_provider(
            auth_provider=BareAuthProvider(
                access_token='test_access_token',
                server_hostname='test_host_name',
                http_path='test_sql_endpoint_path',
                cluster_type=ClusterType.SQL_WAREHOUSE,
            ),
            az_cred_provider=MockDefaultAzureCredential(),
        )

    def test_bare_initiation(self):
        assert self.bare_client.credential_provider.get_credentials() == ClusterCredentials(
            access_token='test_access_token',
            server_hostname='test_host_name',
            http_path='test_sql_endpoint_path',
        )
