import datetime

import numpy as np

from dbxio import types


def test_tinyint():
    assert types.TinyIntType().fit(1)
    assert types.TinyIntType().fit(0)
    assert types.TinyIntType().fit(-1)
    assert types.TinyIntType().fit(127)
    assert types.TinyIntType().fit(-128)
    assert types.TinyIntType().fit(np.int8(0))
    assert types.TinyIntType().fit(np.int8(127))
    assert types.TinyIntType().fit(np.int8(-127))

    assert not types.TinyIntType().fit(1000)
    assert not types.TinyIntType().fit(-1000)
    assert not types.TinyIntType().fit(1.1)
    assert not types.TinyIntType().fit('a')
    assert not types.TinyIntType().fit(None)

    assert types.TinyIntType().serialize(1) == '1Y'
    assert types.TinyIntType().serialize(0) == '0Y'
    assert types.TinyIntType().serialize(-1) == '-1Y'
    assert types.TinyIntType().serialize(127) == '127Y'
    assert types.TinyIntType().serialize(-128) == '-128Y'
    assert types.TinyIntType().serialize(None) == 'NULL'
    assert types.TinyIntType().serialize(np.int8(0)) == '0Y'
    assert types.TinyIntType().serialize(np.int8(127)) == '127Y'
    assert types.TinyIntType().serialize(np.int8(-127)) == '-127Y'

    assert types.TinyIntType().deserialize('1Y') == 1
    assert types.TinyIntType().deserialize('0Y') == 0
    assert types.TinyIntType().deserialize('-1Y') == -1
    assert types.TinyIntType().deserialize('127Y') == 127
    assert types.TinyIntType().deserialize('-128Y') == -128
    assert types.TinyIntType().deserialize('NULL') is None


def test_smallint():
    assert types.SmallIntType().fit(1)
    assert types.SmallIntType().fit(0)
    assert types.SmallIntType().fit(-1)
    assert types.SmallIntType().fit(32767)
    assert types.SmallIntType().fit(-32768)
    assert types.SmallIntType().fit(np.int16(0))
    assert types.SmallIntType().fit(np.int16(32767))
    assert types.SmallIntType().fit(np.int16(-32768))

    assert not types.SmallIntType().fit(100000)
    assert not types.SmallIntType().fit(-100000)
    assert not types.SmallIntType().fit(1.1)
    assert not types.SmallIntType().fit('a')
    assert not types.SmallIntType().fit(None)

    assert types.SmallIntType().serialize(1) == '1S'
    assert types.SmallIntType().serialize(0) == '0S'
    assert types.SmallIntType().serialize(-1) == '-1S'
    assert types.SmallIntType().serialize(32767) == '32767S'
    assert types.SmallIntType().serialize(-32768) == '-32768S'
    assert types.SmallIntType().serialize(None) == 'NULL'
    assert types.SmallIntType().serialize(np.int16(0)) == '0S'
    assert types.SmallIntType().serialize(np.int16(32767)) == '32767S'
    assert types.SmallIntType().serialize(np.int16(-32767)) == '-32767S'

    assert types.SmallIntType().deserialize('1S') == 1
    assert types.SmallIntType().deserialize('0S') == 0
    assert types.SmallIntType().deserialize('-1S') == -1
    assert types.SmallIntType().deserialize('32767S') == 32767
    assert types.SmallIntType().deserialize('-32768S') == -32768
    assert types.SmallIntType().deserialize('NULL') is None


def test_int():
    assert types.IntType().fit(1)
    assert types.IntType().fit(0)
    assert types.IntType().fit(-1)
    assert types.IntType().fit(2147483647)
    assert types.IntType().fit(-2147483648)
    assert types.IntType().fit(np.int32(0))
    assert types.IntType().fit(np.int32(2147483647))
    assert types.IntType().fit(np.int32(-2147483648))

    assert not types.IntType().fit(10000000000)
    assert not types.IntType().fit(-10000000000)
    assert not types.IntType().fit(1.1)
    assert not types.IntType().fit('a')
    assert not types.IntType().fit(None)

    assert types.IntType().serialize(1) == '1'
    assert types.IntType().serialize(0) == '0'
    assert types.IntType().serialize(-1) == '-1'
    assert types.IntType().serialize(2147483647) == '2147483647'
    assert types.IntType().serialize(-2147483648) == '-2147483648'
    assert types.IntType().serialize(None) == 'NULL'
    assert types.IntType().serialize(np.int32(0)) == '0'
    assert types.IntType().serialize(np.int32(2147483647)) == '2147483647'
    assert types.IntType().serialize(np.int32(-2147483647)) == '-2147483647'

    assert types.IntType().deserialize('1') == 1
    assert types.IntType().deserialize('0') == 0
    assert types.IntType().deserialize('-1') == -1
    assert types.IntType().deserialize('2147483647') == 2147483647
    assert types.IntType().deserialize('-2147483648') == -2147483648
    assert types.IntType().deserialize('NULL') is None


def test_bigint():
    assert types.BigIntType().fit(1)
    assert types.BigIntType().fit(0)
    assert types.BigIntType().fit(-1)
    assert types.BigIntType().fit(9223372036854775807)
    assert types.BigIntType().fit(-9223372036854775808)
    assert types.BigIntType().fit(np.int64(0))
    assert types.BigIntType().fit(np.int64(9223372036854775807))
    assert types.BigIntType().fit(np.int64(-9223372036854775808))

    assert not types.BigIntType().fit(100000000000000000000)
    assert not types.BigIntType().fit(-100000000000000000000)
    assert not types.BigIntType().fit(1.1)
    assert not types.BigIntType().fit('a')
    assert not types.BigIntType().fit(None)

    assert types.BigIntType().serialize(1) == '1L'
    assert types.BigIntType().serialize(0) == '0L'
    assert types.BigIntType().serialize(-1) == '-1L'
    assert types.BigIntType().serialize(9223372036854775807) == '9223372036854775807L'
    assert types.BigIntType().serialize(-9223372036854775808) == '-9223372036854775808L'
    assert types.BigIntType().serialize(None) == 'NULL'
    assert types.BigIntType().serialize(np.int64(0)) == '0L'
    assert types.BigIntType().serialize(np.int64(9223372036854775807)) == '9223372036854775807L'
    assert types.BigIntType().serialize(np.int64(-9223372036854775807)) == '-9223372036854775807L'

    assert types.BigIntType().deserialize('1L') == 1
    assert types.BigIntType().deserialize('0L') == 0
    assert types.BigIntType().deserialize('-1L') == -1
    assert types.BigIntType().deserialize('9223372036854775807L') == 9223372036854775807
    assert types.BigIntType().deserialize('-9223372036854775808L') == -9223372036854775808
    assert types.BigIntType().deserialize('NULL') is None


def test_decimal():
    from decimal import Decimal

    assert types.DecimalType().fit(1)
    assert types.DecimalType().fit(0)
    assert types.DecimalType().fit(-1)
    assert types.DecimalType().fit(1.1)
    assert types.DecimalType().fit(-1.1)
    assert types.DecimalType().fit(Decimal('0.11'))

    assert not types.DecimalType().fit('a')
    assert not types.DecimalType().fit(None)

    assert types.DecimalType().serialize(1) == '1BD'
    assert types.DecimalType().serialize(0) == '0BD'
    assert types.DecimalType().serialize(-1) == '-1BD'
    assert types.DecimalType().serialize(1.1) == '1.1BD'
    assert types.DecimalType().serialize(-1.1) == '-1.1BD'
    assert types.DecimalType().serialize(Decimal('0.11')) == '0.11BD'
    assert types.DecimalType().serialize(None) == 'NULL'

    assert types.DecimalType().deserialize('1BD') == 1
    assert types.DecimalType().deserialize('0BD') == 0
    assert types.DecimalType().deserialize('-1BD') == -1
    assert types.DecimalType().deserialize('1.1BD') == 1.1
    assert types.DecimalType().deserialize('-1.1BD') == -1.1
    assert types.DecimalType().deserialize('NULL') is None


def test_float():
    assert types.FloatType().fit(float('-inf'))
    assert types.FloatType().fit(float('inf'))
    assert types.FloatType().fit(1)
    assert types.FloatType().fit(0)
    assert types.FloatType().fit(-1)
    assert types.FloatType().fit(1.1)
    assert types.FloatType().fit(-1.1)
    assert types.FloatType().fit(-3.402e38)
    assert types.FloatType().fit(-1.175e-37)
    assert types.FloatType().fit(1.175e-37)
    assert types.FloatType().fit(3.402e38)
    assert types.FloatType().fit(np.float32(0))
    assert types.FloatType().fit(np.float32(1.1))
    assert types.FloatType().fit(np.float32(-1.1))
    assert types.FloatType().fit(np.float32(-3.401e38))
    assert types.FloatType().fit(np.float32(+3.401e38))

    assert not types.FloatType().fit('a')
    assert not types.FloatType().fit(None)

    assert types.FloatType().serialize(1) == '1F'
    assert types.FloatType().serialize(0) == '0F'
    assert types.FloatType().serialize(-1) == '-1F'
    assert types.FloatType().serialize(1.1) == '1.1F'
    assert types.FloatType().serialize(-1.1) == '-1.1F'
    assert types.FloatType().serialize(None) == 'NULL'
    assert types.FloatType().serialize(float('inf')) == "float('inf')"
    assert types.FloatType().serialize(float('-inf')) == "float('-inf')"

    assert types.FloatType().deserialize('1F') == 1
    assert types.FloatType().deserialize('0F') == 0
    assert types.FloatType().deserialize('-1F') == -1
    assert types.FloatType().deserialize('1.1F') == 1.1
    assert types.FloatType().deserialize('-1.1F') == -1.1
    assert types.FloatType().deserialize('NULL') is None
    assert types.FloatType().deserialize("float('inf')") == float('inf')
    assert types.FloatType().deserialize("float('-inf')") == float('-inf')


def test_double():
    assert types.DoubleType().fit(float('-inf'))
    assert types.DoubleType().fit(float('inf'))
    assert types.DoubleType().fit(1)
    assert types.DoubleType().fit(0)
    assert types.DoubleType().fit(-1)
    assert types.DoubleType().fit(1.1)
    assert types.DoubleType().fit(-1.1)
    assert types.DoubleType().fit(-1.79769e308)
    assert types.DoubleType().fit(-2.225e-307)
    assert types.DoubleType().fit(+2.225e-307)
    assert types.DoubleType().fit(+1.79769e308)
    assert types.DoubleType().fit(np.float64(-1.79769e308))
    assert types.DoubleType().fit(np.float64(-2.225e-307))
    assert types.DoubleType().fit(np.float64(+2.225e-307))
    assert types.DoubleType().fit(np.float64(+1.79769e308))

    assert not types.DoubleType().fit('a')
    assert not types.DoubleType().fit(None)

    assert types.DoubleType().serialize(1) == '1D'
    assert types.DoubleType().serialize(0) == '0D'
    assert types.DoubleType().serialize(-1) == '-1D'
    assert types.DoubleType().serialize(1.1) == '1.1D'
    assert types.DoubleType().serialize(-1.1) == '-1.1D'
    assert types.DoubleType().serialize(None) == 'NULL'
    assert types.DoubleType().serialize(float('inf')) == "double('inf')"
    assert types.DoubleType().serialize(float('-inf')) == "double('-inf')"
    assert types.DoubleType().serialize(np.float64(1.1)) == '1.1D'
    assert types.DoubleType().serialize(np.float64(-1.1)) == '-1.1D'
    assert types.DoubleType().serialize(np.float64(-1.79769e308)) == '-1.79769e+308D'
    assert types.DoubleType().serialize(np.float64(-2.225e-307)) == '-2.225e-307D'

    assert types.DoubleType().deserialize('1D') == 1
    assert types.DoubleType().deserialize('0D') == 0
    assert types.DoubleType().deserialize('-1D') == -1
    assert types.DoubleType().deserialize('1.1D') == 1.1
    assert types.DoubleType().deserialize('-1.1D') == -1.1
    assert types.DoubleType().deserialize('NULL') is None
    assert types.DoubleType().deserialize("double('inf')") == float('inf')
    assert types.DoubleType().deserialize("double('-inf')") == float('-inf')


def test_date():
    assert types.DateType().fit(datetime.date(2014, 1, 1))
    assert types.DateType().fit(datetime.date(1, 1, 1))
    assert types.DateType().fit(datetime.date(9999, 12, 31))
    assert types.DateType().fit('2014-1-1')

    assert not types.DateType().fit('a')
    assert not types.DateType().fit(None)
    assert not types.DateType().fit('2014-01-01T00:00:00')

    assert types.DateType().serialize(datetime.date(2014, 1, 1)) == "DATE'2014-01-01'"
    assert types.DateType().serialize(datetime.date(1, 1, 1)) == "DATE'0001-01-01'"

    assert types.DateType().deserialize("DATE'2014-01-01'") == datetime.date(2014, 1, 1)
    assert types.DateType().deserialize("DATE'0001-01-01'") == datetime.date(1, 1, 1)


def test_timestamp():
    assert types.TimestampType().fit(datetime.datetime(2014, 1, 1, 0, 0, 0))
    assert types.TimestampType().fit(datetime.datetime(1, 1, 1, 0, 0, 0))
    assert types.TimestampType().fit(datetime.datetime(9999, 12, 31, 23, 59, 59))
    assert types.TimestampType().fit('2014-01-01 00:01:01')
    assert types.TimestampType().fit('2014-01-01T00:00:00')
    assert types.TimestampType().fit(datetime.date(2014, 1, 1))

    assert not types.TimestampType().fit('a')
    assert not types.TimestampType().fit(None)

    assert types.TimestampType().serialize(datetime.datetime(2014, 1, 1, 0, 0, 0)) == "TIMESTAMP'2014-01-01T00:00:00'"
    assert types.TimestampType().serialize(datetime.datetime(1, 1, 1, 0, 0, 0)) == "TIMESTAMP'0001-01-01T00:00:00'"

    assert types.TimestampType().deserialize("TIMESTAMP'2014-01-01T00:00:00'") == datetime.datetime(2014, 1, 1, 0, 0, 0)
    assert types.TimestampType().deserialize("TIMESTAMP'0001-01-01T00:00:00'") == datetime.datetime(1, 1, 1, 0, 0, 0)


def test_binary():
    assert types.BinaryType().fit('a')
    assert types.BinaryType().fit(b'a')
    assert types.BinaryType().fit('a')

    assert not types.BinaryType().fit(None)
    assert not types.BinaryType().fit(1)

    assert types.BinaryType().serialize('a') == "X'61'"
    assert types.BinaryType().serialize(b'a') == "X'61'"
    assert types.BinaryType().serialize('a') == "X'61'"
    assert types.BinaryType().serialize(None) == 'NULL'

    assert types.BinaryType().deserialize("X'61'") == b'a'


def test_boolean():
    assert types.BooleanType().fit(True)
    assert types.BooleanType().fit(False)

    assert not types.BooleanType().fit(None)
    assert not types.BooleanType().fit(1)
    assert not types.BooleanType().fit('a')

    assert types.BooleanType().serialize(True) == 'true'
    assert types.BooleanType().serialize(False) == 'false'

    assert types.BooleanType().deserialize('true')
    assert not types.BooleanType().deserialize('false')
    assert types.BooleanType().deserialize(True)
    assert not types.BooleanType().deserialize(False)


def test_string():
    assert types.StringType().fit('a')
    assert types.StringType().fit('a')

    assert not types.StringType().fit(None)
    assert not types.StringType().fit(1)

    assert types.StringType().serialize('a') == '"a"'
    assert types.StringType().serialize('a') == '"a"'
    assert types.StringType().serialize(None) == 'NULL'
    assert types.StringType().serialize('"abc\\"def\\"xyz{\'"') == '"\\"abc\\\\\\"def\\\\\\"xyz{\'\\""'

    assert types.StringType().deserialize('a') == 'a'


def test_null():
    assert types.NullType().fit(None)

    assert not types.NullType().fit(1)
    assert not types.NullType().fit('a')

    assert types.NullType().serialize(None) == 'NULL'

    assert types.NullType().deserialize('NULL') is None


def test_json():
    assert types.JSONType().fit({'a': 1})
    assert types.JSONType().fit([1, 2, 3])
    assert types.JSONType().fit(1)
    assert types.JSONType().fit('a')
    assert types.JSONType().fit(None)

    assert not types.JSONType().fit(datetime.datetime(2014, 1, 1, 0, 0, 0))

    assert types.JSONType().serialize({'a': 1}) == '"{\\"a\\": 1}"'
    assert types.JSONType().serialize([1, 2, 3]) == '"[1, 2, 3]"'
    assert types.JSONType().serialize(1) == '"1"'
    assert types.JSONType().serialize('a') == '"\\"a\\""'
    assert types.JSONType().serialize(None) == 'NULL'

    assert types.JSONType().deserialize('{"a": 1}') == {'a': 1}
    assert types.JSONType().deserialize('[1, 2, 3]') == [1, 2, 3]
    assert types.JSONType().deserialize('1') == 1
    assert types.JSONType().deserialize('"a"') == 'a'
    assert types.JSONType().deserialize('NULL') is None


def test_variant():
    assert types.VariantType().fit(1)
    assert types.VariantType().fit('a')
    assert types.VariantType().fit(None)
    assert types.VariantType().fit(datetime.datetime(2014, 1, 1, 0, 0, 0))
    assert types.VariantType().fit([1, 2, 3])
    assert types.VariantType().fit({'a': 1})
