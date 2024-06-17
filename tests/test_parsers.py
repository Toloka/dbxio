import unittest

import numpy as np

try:
    import pydantic.v1 as pydantic
except ModuleNotFoundError:
    import pydantic

from dbxio import TableSchema
from dbxio.delta.parsers import infer_schema
from dbxio.sql import as_dbxio_type, types


class TestTableSchema(unittest.TestCase):
    def setUp(self) -> None:
        self.basic_schema_as_list = [
            {'name': 'col_int', 'type': types.IntType()},
            {'name': 'col_string', 'type': types.StringType()},
            {'name': 'col_boolean', 'type': types.BooleanType()},
            {'name': 'col_array', 'type': types.ArrayType(types.IntType())},
            {'name': 'col_map', 'type': types.MapType(types.StringType(), types.IntType())},
            {'name': 'col_json', 'type': types.JSONType()},
        ]

    def test_correct_schema_parsing(self):
        TableSchema(self.basic_schema_as_list)

    def test_incorrect_schema_parsing(self):
        broken_schema = list(self.basic_schema_as_list)
        broken_schema.append({'column_name': 'col123', 'column_type': 'type987'})

        with self.assertRaises(pydantic.ValidationError):
            TableSchema(broken_schema)

    def test_convert_schema_to_applicable_dict(self):
        new_schema = TableSchema(self.basic_schema_as_list).as_dict()

        exp_parsed_schema = {
            'col_int': types.IntType(),
            'col_string': types.StringType(),
            'col_boolean': types.BooleanType(),
            'col_array': types.ArrayType(types.IntType()),
            'col_map': types.MapType(types.StringType(), types.IntType()),
            'col_json': types.JSONType(),
        }
        self.assertDictEqual(new_schema, exp_parsed_schema)

    def test_convert_schema_to_inapplicable_dict(self):
        broken_schema = list(self.basic_schema_as_list)
        broken_schema.append({'column_name': 'col123', 'column_type': 'type987'})

        with self.assertRaises(pydantic.ValidationError):
            TableSchema(broken_schema)

    def test_parse_and_apply_correct_schema(self):
        test_correct_record = {
            'col_int': 1,
            'col_string': 'foobar',
            'col_boolean': True,
            'col_array': [1, 2, 42],
            'col_map': [('a', 1), ('b', 2)],
            'col_json': '{"k1": "v1", "k2": true, "k3": 1.1, "k4": 42}',
        }
        schema = TableSchema(self.basic_schema_as_list)

        parsed_record = schema.apply(test_correct_record)

        exp_parsed_record = {
            'col_int': 1,
            'col_string': 'foobar',
            'col_boolean': True,
            'col_array': [1, 2, 42],
            'col_map': {'a': 1, 'b': 2},
            'col_json': {'k1': 'v1', 'k2': True, 'k3': 1.1, 'k4': 42},
        }

        self.assertDictEqual(parsed_record, exp_parsed_record)

    def test_parse_and_apply_incorrect_schema(self):
        test_correct_record = {
            'col_int': 1,
            'col_string': 'foobar',
            'col_boolean': True,
            'col_array': [1, 2, 42],
            'col_map': "[('a', 1), ('b', 2)]",
            'col_json': '{"k1": "v1", "k2": true, "k3": 1.1, "k4": 42}',
        }
        schema = TableSchema(self.basic_schema_as_list).as_dict()

        with self.assertRaises(Exception):
            schema.apply(test_correct_record)

    def test_try_find_smallest_type(self):
        assert isinstance(as_dbxio_type(1), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(32_700), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(2**30), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(2**62), types.BigIntType())  # noqa
        assert isinstance(as_dbxio_type(3.402e38 - 1), types.FloatType())  # noqa
        assert isinstance(as_dbxio_type(1.79769e308), types.DoubleType())  # noqa
        assert isinstance(as_dbxio_type([1, 2, 3]), types.ArrayType(types.IntType()))  # noqa
        assert isinstance(as_dbxio_type({'a': 1, 'b': 2}), types.MapType(types.StringType(), types.IntType()))  # noqa

    def test_as_dbxio_type_numpy(self):
        assert isinstance(as_dbxio_type(np.int8(1)), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(np.int32(32_700)), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(np.int32(2**30)), types.IntType())  # noqa
        assert isinstance(as_dbxio_type(np.int64(2**62)), types.BigIntType())  # noqa
        assert isinstance(as_dbxio_type(np.float32(3.401e38)), types.FloatType())  # noqa
        assert isinstance(as_dbxio_type(np.float64(1.79769e308)), types.DoubleType())  # noqa

    def test_array_parsing(self):
        import datetime

        assert types.ArrayType(types.StringType()).serialize(['a', 'b', 'c']) == 'ARRAY("a", "b", "c")'
        assert types.ArrayType(types.IntType()).serialize([1, 2, 3]) == 'ARRAY(1, 2, 3)'
        assert types.ArrayType(types.DateType()).serialize([datetime.date(2023, 1, 1)]) == "ARRAY(DATE'2023-01-01')"
        with self.assertRaises(Exception):
            types.ArrayType(types.IntType()).serialize([1, 2, 3, 'a'])

    def test_nullable_columns(self):
        assert types.IntType().serialize(None) == 'NULL'
        assert types.FloatType().serialize(None) == 'NULL'
        assert types.ArrayType(types.StringType()).serialize(None) == 'NULL'
        assert types.StringType().serialize(None) == 'NULL'

        assert types.IntType().serialize(1) == '1'
        assert types.FloatType().serialize(1.1) == '1.1F'
        assert types.ArrayType(types.TinyIntType()).serialize([42]) == 'ARRAY(42Y)'
        assert types.StringType().serialize('abc"') == '"abc\\""'

    def test_map_parsing(self):
        assert types.MapType(types.StringType(), types.IntType()).serialize({'a': 1, 'b': 2}) == "MAP('a', 1, 'b', 2)"
        assert (
            types.MapType(types.StringType(), types.StringType()).serialize({'a': 'b', 'c': 'd'})
            == "MAP('a', 'b', 'c', 'd')"
        )

    def test_infer_schema(self):
        record = {
            'col_int': 1,
            'col_string': 'foobar',
            'col_boolean': True,
            'col_array': [1, 2, 42],
            'col_map': {'a': 1, 'b': 2},
            'col_struct': {'k1': 'v1', 'k2': True, 'k3': 1.1, 'k4': 42},
        }
        schema = infer_schema(record)

        exp_parsed_schema = {
            'col_int': types.IntType(),
            'col_string': types.StringType(),
            'col_boolean': types.BooleanType(),
            'col_array': types.ArrayType(types.IntType()),
            'col_map': types.MapType(types.StringType(), types.IntType()),
            'col_struct': types.StructType(
                [
                    ('k1', types.StringType()),
                    ('k2', types.BooleanType()),
                    ('k3', types.FloatType()),
                    ('k4', types.IntType()),
                ]
            ),
        }
        self.assertDictEqual(schema.as_dict(), exp_parsed_schema)
