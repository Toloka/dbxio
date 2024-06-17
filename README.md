[![PyPI - Version](https://img.shields.io/pypi/v/dbxio)](https://pypi.org/project/dbxio/)
[![GitHub Build](https://github.com/Toloka/dbxio/workflows/Tests/badge.svg)](https://github.com/Toloka/dbxio/actions)

[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbxio.svg)](https://pypi.org/project/dbxio/)
[![PyPI - Downloads](https://img.shields.io/pepy/dt/dbxio)](https://pypi.org/project/dbxio/)

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

# dbxio: High-level Databricks client

## Overview

**_dbxio_** is a high-level client for Databricks that simplifies working with tables and volumes.
It provides a simple interface for reading and writing data, creating and deleting objects, and running SQL queries and
fetching results.

## Why _dbxio_?

1. **_dbxio_** connects the power of Databricks SQL and Python for local data manipulation.
2. **_dbxio_** provides a simple and intuitive interface for working with Databricks Tables and Volumes.
   Now it's possible to read/write data with just a few lines of code.
3. For large amounts of data, **_dbxio_** uses intermediate object storage of your choice to perform bulk upload later
   (see [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) for more details).
   So, you can upload any amount of data, and _dbxio_ will take care of synchronizing the data with the table in
   Databricks.

### Alternatives

Currently, we are not aware of any alternatives that offer the same functionality as **_dbxio_**.
If you come across any, we would be interested to learn about them.
Please let us know by opening an issue in our GitHub repository.

---

## Installation

**_dbxio_** requires Python 3.9 or later. You can install **_dbxio_** using pip:

```bash
pip install dbxio
```

## _dbxio_ by Example

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
    cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
    http_path='<YOUR_HTTP_PATH>',
    server_hostname='<YOUR_SERVER_HOSTNAME>',
    settings=dbxio.Settings(cloud_provider=dbxio.CloudProvider.AZURE),
)

# read table
table = list(dbxio.read_table('catalog.schema.table', client=client))

# write table
data = [
    {'col1': 1, 'col2': 'a', 'col3': [1, 2, 3]},
    {'col1': 2, 'col2': 'b', 'col3': [4, 5, 6]},
]
schema = dbxio.TableSchema.from_obj(
    {
        'col1': dbxio.types.IntType(),
        'col2': dbxio.types.StringType(),
        'col3': dbxio.types.ArrayType(dbxio.types.IntType()),
    }
)
dbxio.bulk_write_table(
    dbxio.Table('domain.schema.table', schema=schema),
    data,
    client=client,
    abs_name='blob_storage_name',
    abs_container_name='container_name',
    append=True,
)
```

---

## Cloud Support

**_dbxio_** supports the following cloud providers:

- [x] Azure
- [ ] AWS (in plans)
- [ ] GCP (in plans)

## Project Information

- [Docs](docs/README.md)
- [PyPI](https://pypi.org/project/dbxio/)
- [Contributing](CONTRIBUTING.md)
