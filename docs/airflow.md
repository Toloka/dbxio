# _dbxio_ with Airflow

When using Airflow, it can be useful to use a default _dbxio_ client to access app-purpose clusters and sql warehouses
that are preconfigured for all Airflow users.

## Prerequisites

To use default clusters, you need to:

1. Set in Airflow environment variable with name `AIRFLOW_UNIQUE_NAME` to identify your Airflow instance.
2. Set an Airflow Variable with name `<AIRFLOW_UNIQUE_NAME>-databricks_default_sql_http_path` to the HTTP path of the
   default all-purpose cluster.
3. Set an Airflow Variable with name `<AIRFLOW_UNIQUE_NAME>-databricks_default_warehouse_sql_http_path` to the HTTP path
   of the default SQL warehouse.
4. Set an Airflow Variable with name `<AIRFLOW_UNIQUE_NAME>-databricks_default_host` to the hostname of the Databricks
   workspace.

After setting these variables, you can use default _dbxio_ clients in your Airflow DAGs. There would be no need to
configure the clients with the cluster and warehouse paths in each DAG.

## Usage

```python
from dbxio import DefaultSqlDbxIOClient, DefaultDbxIOClient

all_purpose_client = DefaultDbxIOClient()

warehouse_client = DefaultSqlDbxIOClient()
```