import datetime

from dbxio import types


def test_array():
    assert types.ArrayType(types.IntType()).fit([1, 2, 3])
    assert types.ArrayType(types.StringType()).fit(['a', 'b', 'c'])
    assert types.ArrayType(types.StringType()).fit([])

    assert not types.ArrayType(types.IntType()).fit([1, 2, 'a'])
    assert not types.ArrayType(types.StringType()).fit(['a', 'b', 1])
    assert not types.ArrayType(types.IntType()).fit([1, 2.1, 3])

    assert types.ArrayType(types.IntType()).serialize([1, 2, 3]) == 'ARRAY(1, 2, 3)'
    assert types.ArrayType(types.StringType()).serialize(['a', 'b', 'c']) == 'ARRAY("a", "b", "c")'
    assert types.ArrayType(types.StringType()).serialize([]) == 'ARRAY()'

    assert types.ArrayType(types.IntType()).deserialize('ARRAY(1, 2, 3)') == [1, 2, 3]
    assert types.ArrayType(types.StringType()).deserialize('ARRAY("a", "b", "c")') == ['a', 'b', 'c']
    assert types.ArrayType(types.StringType()).deserialize('ARRAY()') == []
    assert types.ArrayType(types.ArrayType(types.IntType())).deserialize('ARRAY(ARRAY(1, 2, 3), ARRAY(4, 5, 6))') == [
        [1, 2, 3],
        [4, 5, 6],
    ]
    assert types.ArrayType(types.ArrayType(types.StringType())).deserialize(
        'ARRAY(ARRAY("a", "b", "c"), ARRAY("d", "e", "f"))'
    ) == [['a', 'b', 'c'], ['d', 'e', 'f']]
    assert types.ArrayType(types.DateType()).deserialize(
        "ARRAY(DATE'2019-01-01', DATE'2019-01-02', DATE'2019-01-03')"
    ) == [datetime.date(2019, 1, 1), datetime.date(2019, 1, 2), datetime.date(2019, 1, 3)]


def test_map():
    assert types.MapType(types.StringType(), types.IntType()).fit({'a': 1, 'b': 2, 'c': 3})
    assert types.MapType(types.StringType(), types.StringType()).fit({'a': '1', 'b': '2', 'c': '3'})

    assert not types.MapType(types.StringType(), types.IntType()).fit({'a': 1, 'b': 2, 'c': '3'})
    assert not types.MapType(types.StringType(), types.StringType()).fit({'a': '1', 'b': '2', 'c': 3})

    assert (
        types.MapType(types.StringType(), types.IntType()).serialize({'a': 1, 'b': 2, 'c': 3})
        == "MAP('a', 1, 'b', 2, 'c', 3)"
    )


def test_struct():
    assert types.StructType([('a', types.IntType()), ('b', types.StringType())]).fit({'a': 1, 'b': '2'})
    assert not types.StructType([('a', types.IntType()), ('b', types.StringType())]).fit({'a': 1})

    complex_nested_type = types.ArrayType(
        types.StructType(
            [
                ('name', types.StringType()),
                ('col', types.StructType([('a', types.IntType()), ('b', types.ArrayType(types.StringType()))])),
            ]
        )
    )
    assert complex_nested_type.fit(
        [{'name': 'a', 'col': {'a': 1, 'b': ['b', 'b']}}, {'name': 'aa', 'col': {'a': 2, 'b': ['bb', 'bb']}}]
    )
    assert not complex_nested_type.fit(
        [{'name': 'a', 'col': {'a': 1, 'b': ['b', 'b']}}, {'name': 'aa', 'col': {'a': 2, 'b': ['bb', 2]}}]
    )

    assert complex_nested_type.serialize(
        [{'name': 'a', 'col': {'a': 1, 'b': ['b', 'b']}}, {'name': 'aa', 'col': {'a': 2, 'b': ['bb', 'bb']}}]
    ) == (
        'ARRAY(named_struct("name","a","col",named_struct("a",1,"b",ARRAY("b", "b"))), '
        'named_struct("name","aa","col",named_struct("a",2,"b",ARRAY("bb", "bb"))))'
    )
