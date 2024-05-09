from typing import Any, Dict

from dbxio.delta.table_schema import TableSchema
from dbxio.delta.types import as_dbxio_type


def infer_schema(record: Dict[str, Any]) -> TableSchema:
    """
    Infer schema from a record. Optimal data types will be used.
    """
    schema = []
    for key, value in record.items():
        col = {'name': key, 'type': as_dbxio_type(value)}
        schema.append(col)
    return TableSchema(schema)  # type: ignore
