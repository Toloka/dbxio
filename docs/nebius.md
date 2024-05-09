# Nebius over Azure

## Create _dbxio_ client

It's important to preset Azure credential provider for _dbxio_ client.

```python
import dbxio
from azure.identity import AzureCliCredential

# create preferred Azure credential provider
credential = AzureCliCredential()

client = dbxio.DbxIOClient.from_cluster_settings(
   cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
   http_path='sql/protocolv1/o/12345678900000/abcdefg-12345',
   server_hostname='adb-98765423456.11.azuredatabricks.net',
   az_cred_provider=credential,
)
```

Or you can use a default client for Nebius if all environment variables are set.

```python
from dbxio import DefaultNebiusSqlClient

# assuming env variables DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HTTP_PATH are set
client = DefaultNebiusSqlClient()
```
