import json
import random
import string

import pyarrow as pa

from dbxio.blobs.parquet import create_pa_table
from dbxio.delta.table_schema import TableSchema
from dbxio.delta.types import ArrayType, IntType, JSONType, StringType


def test_create_pa_table():
    size = 100
    alphabet = string.ascii_letters

    batch = {
        'col_int': [random.randint(0, 10) for _ in range(size)],
        'col_str': [random.choice(alphabet) for _ in range(size)],
        'col_dict': [json.dumps({'key': 'value', 'key2': {'in_val1': 1, 'in_val2': [1, 2, -42]}}) for _ in range(size)],
        'col_list': [[1, 2, 42] for _ in range(size)],
    }
    schema = TableSchema(
        [
            {'name': 'col_int', 'type': IntType()},
            {'name': 'col_str', 'type': StringType()},
            {'name': 'col_dict', 'type': JSONType()},
            {'name': 'col_list', 'type': ArrayType(IntType())},
        ]
    )

    table = create_pa_table(batch, schema)

    assert table.column_names == ['col_int', 'col_str', 'col_dict', 'col_list']
    assert [col.type for col in table.columns] == [pa.int32(), pa.string(), pa.string(), pa.list_(pa.int32())]
    assert table.shape == (len(batch['col_str']), len(batch))
    assert table['col_str'].to_pylist() == batch['col_str']
    assert table['col_int'].to_pylist() == batch['col_int']
    assert table['col_dict'].to_pylist() == batch['col_dict']
    assert table['col_list'].to_pylist() == batch['col_list']
