from dbxio import TableSchema, types


def test_table_schema():
    TableSchema(
        [
            {'name': 'col1', 'type': types.StringType()},
            {'name': 'col2', 'type': types.IntType()},
        ]
    )


def test_table_schema_from_list():
    TableSchema.from_obj([{'name': 'col1', 'type': types.StringType()}])


def test_table_schema_from_dict():
    TableSchema.from_obj({'col1': types.StringType()})


def test_schema_columns():
    schema = TableSchema.from_obj({'a': types.StringType(), 'b': types.IntType()})
    assert schema.columns == ['a', 'b']


def test_schema_as_dict():
    schema = TableSchema.from_obj({'a': types.StringType(), 'b': types.IntType()})
    assert schema.as_dict() == {'a': types.StringType(), 'b': types.IntType()}


def test_schema_apply():
    schema = TableSchema.from_obj({'a': types.StringType(), 'b': types.IntType()})
    assert schema.apply({'a': 'foo', 'b': 42}) == {'a': 'foo', 'b': 42}
