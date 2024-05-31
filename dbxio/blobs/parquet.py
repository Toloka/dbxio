import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, Iterator, Union

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import ContainerClient

from dbxio.sql.types import convert_dbxio_type_to_pa_type

if TYPE_CHECKING:
    from dbxio.delta.table import Table
    from dbxio.delta.table_schema import TableSchema

ROW_GROUP_SIZE_BYTES = 128 * 2**20


def create_pa_table(parsed_micro_batch: Dict, schema: 'TableSchema') -> pa.Table:
    batch_as_arrays = []
    schema_as_dict = schema.as_dict()
    for col_name, col_values in parsed_micro_batch.items():
        col_type = convert_dbxio_type_to_pa_type(schema_as_dict[col_name])
        batch_as_arrays.append(pa.array(col_values, type=col_type))

    return pa.Table.from_arrays(arrays=batch_as_arrays, names=list(parsed_micro_batch.keys()))


def pa_table2parquet(table: pa.Table) -> bytes:
    stream = pa.BufferOutputStream()
    row_group_size = int(table.num_rows / int(table.nbytes / ROW_GROUP_SIZE_BYTES + 1) + 1)
    pq.write_table(table, stream, flavor={'spark'}, row_group_size=row_group_size)
    return stream.getvalue().to_pybytes()


def arrow_stream2parquet(stream: bytes) -> bytes:
    table = pa.ipc.open_stream(stream).read_all()
    return pa_table2parquet(table)


@contextmanager
def create_tmp_parquet(data: bytes, table_identifier: Union[str, 'Table'], client: 'ContainerClient') -> Iterator[str]:
    random_part = uuid.uuid4()
    ti = table_identifier if isinstance(table_identifier, str) else table_identifier.table_identifier
    translated_table_identifier = ti.translate(
        str.maketrans('.!"#$%&\'()*+,/:;<=>?@[\\]^`{|}~', '______________________________')
    )
    tmp_path = f'{translated_table_identifier}__dbxio_tmp__{random_part}.parquet'
    client.upload_blob(name=tmp_path, data=data, overwrite=True)
    try:
        yield tmp_path
    finally:
        client.delete_blob(tmp_path)
