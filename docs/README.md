# _dbxio_ Tutorial

- [Prerequisites](#prerequisites)
- [Create `dbxio` client](#create-dbxio-client)
- [Specify cloud provider](#specify-cloud-provider)
- [Basic read/write table operations](#basic-readwrite-table-operations)
    - [Read table](#read-table)
    - [Write table](#write-table)
    - [Run SQL query and fetch results](#run-sql-query-and-fetch-results)
    - [Save results to files](#save-results-to-files)
    - [Save SQL results to files](#save-sql-results-to-files)
    - [Upload large files to Databricks table](#upload-large-files-to-databricks-table)
- [Volume operations](#volume-operations)
    - [Upload to Volume non-tabular data](#upload-to-volume-non-tabular-data)
    - [Download from Volume](#download-from-volume)
- [Further docs](#further-docs)

## Prerequisites

1. Login to Azure using `az cli` ([installation](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli))

```bash
az login --scope 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default --use-device-code
```

This will prompt you with a link and code to open in any external browser.
<details>
<summary>What is 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default?</summary>

This is the default scope for Azure Databricks. It is used to access the Databricks API. You don't need to change it.

[Docs](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/service-prin-aad-token)

</details>

2. Find your http path and server hostname in Databricks workspace.

   It can be found on your target cluster's page, in section `Advanced Options` --> `JDBC/ODBC`

## Create `dbxio` client

There are several ways to create a `dbxio` client:

- presetting `http_path` and `server_hostname`. In this case access token will be obtained from the environment
  variable `DATABRICKS_ACCESS_TOKEN` (if available) or from a credential provider.

```python
from dbxio import DbxIOClient, ClusterType

client = DbxIOClient.from_cluster_settings(
    cluster_type=ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
)
```

- using default clients for SQL Warehouse and all-purpose cluster. Can be useful if http path and server hostname are
  stored in environment variables.

```python
import os
from dbxio import DefaultSqlDbxIOClient, DefaultDbxIOClient

os.environ['DATABRICKS_HTTP_PATH'] = '<YOUR_HTTP_PATH>'
os.environ['DATABRICKS_SERVER_HOSTNAME'] = '<YOUR_SERVER_HOSTNAME>'

sql_client = DefaultSqlDbxIOClient()

all_purpose_client = DefaultDbxIOClient()
```

- using PAT token

```python
from dbxio import DbxIOClient, BareAuthProvider, ClusterType

client = DbxIOClient.from_auth_provider(
    auth_provider=BareAuthProvider(
        access_token='dapixxxxxx-xxxxx-xxxxxx-x',
        http_path='<YOUR_HTTP_PATH>',
        server_hostname='<YOUR_SERVER_HOSTNAME>',
        cluster_type=ClusterType.SQL_WAREHOUSE,
    )
)
```

## Specify cloud provider

`dbxio` supports (or will support in the future, see [available cloud providers](../README.md#cloud-support)) cloud
providers supported by Databricks.

To specify the cloud provider, pass settings to the client:

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
    settings=dbxio.Settings(cloud_provider=dbxio.CloudProvider.AZURE),
)
```

### Explicitly specify credential provider

Credential provider is resolved automatically based on the cloud provider. But you can specify it explicitly when
creating a client.

```python
import dbxio
from azure.identity import AzureCliCredential

client = dbxio.DbxIOClient.from_cluster_settings(
    # ...,
    az_cred_provider=AzureCliCredential(),
    # ...,
)
```

## Basic read/write table operations

> [!NOTE]
> In general, it's recommended to use SQL Warehouses for all operations. Using all-purpose clusters for fetching data
> can be extremely slow. Carefully consider the cluster type for your operations.

### Read table

```python
import pandas as pd
from dbxio import read_table

# read all records from table and convert to pandas DataFrame
table = pd.DataFrame(read_table('catalog.schema.table', client=...))

# read only 10 records
table10 = list(read_table('catalog.schema.table', client=..., limit_records=10))

# read record by record
with read_table('catalog.schema.table', client=...) as gen:
    for record in gen:
        ...  # do something with record
```

### Write table

There are two ways to write data to a table: using SQL and using bulk operation.

- SQL: this approach creates one sql query for all records and performs a `INSERT INTO` operation.
  It's slow and not recommended for big amounts of data. There is a strict limit that only 50 Mb of data can be written
  at once.

- Bulk operation: this approach writes data to object storage in parquet format and then performs a `COPY INTO`
  operation.
  It's faster and recommended for big amounts of data. Object storage must be mounted to the Databricks workspace to
  access data.

```python
import dbxio

data = [
    {'col1': 1, 'col2': 'a', 'col3': [1, 2, 3]},
    {'col1': 2, 'col2': 'b', 'col3': [4, 5, 6]},
]
schema = dbxio.TableSchema.from_obj(
    [
        {'name': 'col1', 'type': dbxio.types.IntType()},
        {'name': 'col2', 'type': dbxio.types.StringType()},
        {'name': 'col3', 'type': dbxio.types.ArrayType(dbxio.types.IntType())},
    ]
)

# write data to table using sql (slow and not recommended for big amounts of data)
dbxio.write_table(
    dbxio.Table('catalog.schema.table', schema=schema),
    data,
    client=...,
    append=True,
)

# write data to table using bulk operation
dbxio.bulk_write_table(
    dbxio.Table('catalog.schema.table', schema=schema),
    data,
    client=...,
    abs_name='blob_storage_name',
    abs_container_name='container_name',
    append=True,
)
```

### Run SQL query and fetch results

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
)

# fetch all results
data = list(client.sql('select 1+1'))

# fetch results and convert to pandas DataFrame
df: pd.DataFrame = client.sql('select 1+1').df()

# or you can use a generator
with client.sql('select 1+1') as gen:
    for record in gen:
        ...  # do something with record

```

### Save results to files

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
)
path_to_chunks = client.sql_to_files(
    query='select 1+1',
    results_path='path/to/save/files',
    max_concurrency=8,
)
```

### Save SQL results to files

`dbxio` can save the result of an arbitrary SQL query to files in parquet format.
The result will be chunked into several files.
The returned path will contain a directory named `<statement_id>` where all files will be saved (_statement_id_ is a
unique identifier of the query)

> [!NOTE]
> As usual, it's recommended to use SQL warehouse.

<details>
<summary>Using all-purpose clusters</summary>

All-purpose clusters use ODBC protocol to fetch results of queries, and it can be extremely slow even for small tables.
It's not deprecated, but it's highly recommended to use SQL warehouses instead.

</details>

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
)

path_to_files = dbxio.save_table_to_files(
    table='catalog.schema.table',
    client=client,
    results_path='path/to/save/files',
    max_concurrency=8,
)

# or save a result of an arbitrary SQL query
QUERY = 'select * from domain.schema.table where 1=1 and 2=2'
path_to_files = client.sql_to_files(
    query=QUERY,
    results_path='path/to/save/files',
    max_concurrency=8,
)
```

### Upload large files to Databricks table

Supported formats: `CSV`, `JSON`, `AVRO`, `ORC`, `PARQUET`, `TEXT`, `BINARYFILE`.

> [!WARNING]  
> `dbxio` does not make any transformations to the data. It is the user's responsibility to ensure that the data is
> in the correct format and schema.

```python
import logging
import dbxio
import pandas as pd

logging.basicConfig(level=logging.INFO)

# it can be a path to directory. then all files by glob **/*.<format> will be uploaded (but they must have the same schema)
LARGE_FILE_TO_UPLOAD = 'path/to/large/file.csv'  # 1GB+

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
)
schema = dbxio.infer_schema(pd.read_csv(LARGE_FILE_TO_UPLOAD, low_memory=True).iloc[0].to_dict())
table_format = dbxio.TableFormat.CSV
table = dbxio.Table('catalog.schema.table', schema=schema, table_format=table_format)

dbxio.bulk_write_local_files(
    table=table,
    path=LARGE_FILE_TO_UPLOAD,
    table_format=table_format,
    client=client,
    append=False,
    abs_name='blob_storage_name',
    abs_container_name='container_name',
    max_concurrency=8,
)
```

<details>
<summary>Data consistency</summary>

Under the hood `dbxio` copies all files to ABS tracking success of the upload and writes log files to be resumable
without data loss or repeated upload.

After the upload is finished, `dbxio` runs `COPY INTO` command to load the data into the table.

</details>


<details>
<summary>If something went wrong</summary>

`dbxio` uses blob lease to ensure that there's only one process can write to the blob.
If another process tries to write to the blob, it will raise `LeaseAlreadyPresentError`.

But sometimes lease can be left without a release.
To break all leases, pass `force=True` to `bulk_write_local_files` function.

</details>

## Volume operations

There are two types of Volumes in Databricks: managed and external.
You can read more about them in the
Databricks [documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html).

`dbxio` fully supports both types.

Working with data in external Volume will be done using SDK your cloud provider.
To work with managed Volume `dbxio` uses [Databricks Files API](https://docs.databricks.com/api/workspace/files).

> [!NOTE]
> Databricks API allows downloading/uploading files up to 5GB in managed Volumes.
> If you need to download bigger files, consider using external Volume or splitting the file into smaller parts.

### Upload to Volume non-tabular data

To work with external Volumes in Databricks, you need to make sure that your target
catalog has associated external storage.

Associated external storage is:

- created external location in the Databricks workspace
- stored desired container name in catalog's properties with key `default_external_location`

> [!NOTE]
> `dbxio` creates Volumes automatically on write operations. If you want to disable this behavior,
> pass `create_volume_if_not_exists=False` to `write_volume` function.,

```python
# dbxio will upload all found files in the directory (except "hidden" files)
PATH_TO_FILES = 'path/to/files'
dbxio.write_volume(
    path=PATH_TO_FILES,
    catalog_name='catalog_name',
    schema_name='schema_name',
    volume_name='volume_name',
    client=...,
    volume_type=dbxio.VolumeType.MANAGED,  # or EXTERNAL
    max_concurrency=8,
)
```

#### Upload single file (or files by prefix path) to existing Volume

If you want to add or modify files in existing Volume, you can use `volume_path` parameter to specify the path in
Volume.

The code below will add or modify file `file.txt` on the path `path/in/volume/file.txt` in Volume.
If Volume does not exist, it will be created.

```python
dbxio.write_volume(
    path='path/to/file.txt',
    catalog_name='catalog_name',
    schema_name='schema_name',
    volume_name='volume_name',
    client=...,
    volume_path='path/in/volume',
)
```

### Download from Volume

```python
dbxio.download_volume(
    path='local/path/to/download',
    catalog_name='catalog_name',
    schema_name='schema_name',
    volume_name='volume_name',
    client=...,
)
```

#### Download files by prefix path from Volume

It's also possible to download files by prefix path from Volume.

Code below will download all files from the path `path/in/volume` in Volume to the local
directory `local/path/to/download`.

```python
dbxio.download_volume(
    path='local/path/to/download',
    catalog_name='catalog_name',
    schema_name='schema_name',
    volume_name='volume_name',
    client=...,
    volume_path='path/in/volume',
)
```

### Delete Volume

`dbxio` can delete Volumes in Databricks.
If your Volume is external, it will delete all files in object storage first and then delete the Volume's metadata.

```python
# first we need to create a Volume object (it will fetch all required information from Databricks)
volume = dbxio.Volume.from_url('/Volumes/<catalog>/<schema>/<volume_name>', client=...)

dbxio.delete_volume(volume, client=...)
```

## Further docs

- [Use _dbxio_ on Airflow](./airflow.md)