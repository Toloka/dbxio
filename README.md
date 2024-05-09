[![PyPI - Version](https://img.shields.io/pypi/v/dbxio)](https://pypi.org/project/dbxio/)
[![GitHub Build](https://github.com/Toloka/dbxio/workflows/Tests/badge.svg)](https://github.com/Toloka/dbxio/actions)
<a href="https://pypi.org/project/dbxio" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/dbxio.svg?color=%2334D058" alt="Supported Python versions">
</a>

[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbxio.svg)](https://pypi.org/project/dbxio/)
[![PyPI - Downloads](https://img.shields.io/pepy/dt/dbxio)](https://pypi.org/project/dbxio/)

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

# dbxio: High-level Databricks client

## Overview
_dbxio_ is a high-level client for Databricks that simplifies working with tables and volumes. 
It provides a simple interface for reading and writing data, creating and deleting objects, and running SQL queries and
fetching results.

## Installation
_dbxio_ requires Python 3.9 or later. You can install _dbxio_ using pip:

```bash
pip install dbxio
```

## _dbxio_ by Example

```python
import dbxio

client = dbxio.DbxIOClient.from_cluster_settings(
   cluster_type=dbxio.ClusterType.SQL_WAREHOUSE,
   http_path='sql/protocolv1/o/2350007385231210/abcdefg-12345',
   server_hostname='adb-1234567890.00.azuredatabricks.net',
)

# read table
table = list(dbxio.read_table('catalog.schema.table', client=client))

# write table
data = [
   {'col1': 1, 'col2': 'a', 'col3': [1, 2, 3]},
   {'col1': 2, 'col2': 'b', 'col3': [4, 5, 6]},
]
schema = dbxio.TableSchema(
   [
      {'name': 'col1', 'type': dbxio.types.IntType()},
      {'name': 'col2', 'type': dbxio.types.StringType()},
      {'name': 'col3', 'type': dbxio.types.ArrayType(dbxio.types.IntType())},
   ]
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

## Cloud Support
_dbxio_ supports the following cloud providers:
- [x] Azure
- [x] Nebius over Azure
- [ ] AWS (will be added soon)
- [ ] GCP (will be added soon)

## Project Information
- [Docs](docs/README.md)
- [PyPI](https://pypi.org/project/dbxio/)
- [Contributing](CONTRIBUTING.md)
