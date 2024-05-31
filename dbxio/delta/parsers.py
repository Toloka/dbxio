from typing import Any, Dict

from dbxio.delta.table_schema import TableSchema
from dbxio.sql.types import as_dbxio_type


def infer_schema(record: Dict[str, Any]) -> TableSchema:
    """
    Infer schema from a record. Optimal data types will be used.
    """
    schema = []
    for key, value in record.items():
        col = {'name': key, 'type': as_dbxio_type(value)}
        schema.append(col)
    return TableSchema.from_obj(schema)
