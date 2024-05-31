import datetime
import json
import math
from abc import ABCMeta, abstractmethod
from decimal import Decimal
from typing import Any

import dateutil.parser
import numpy as np
import pyarrow as pa
from decorator import decorator

from dbxio.core.exceptions import DbxIOTypeError
from dbxio.utils.logging import get_logger

logger = get_logger()


@decorator
def nullable(function, *args):
    if args[1] is None:
        return NullType().serialize(args[1])
    if isinstance(args[1], str) and args[1] == 'NULL':
        return NullType().deserialize(args[1])
    return function(*args)


class BaseType(metaclass=ABCMeta):
    def __str__(self):
        """
        Each class name ends with `Type` suffix. This method returns the name without it in the upper case.
        """
        return self.__class__.__name__[:-4].upper()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseType):
            return NotImplemented
        return isinstance(self, other.__class__) and str(self) == str(other)

    def __instancecheck__(self, instance):
        return self.__eq__(instance)

    @abstractmethod
    def fit(self, obj) -> bool:
        raise NotImplementedError

    @abstractmethod
    def serialize(self, obj, unsafe: bool = False):
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, obj):
        raise NotImplementedError

    def raise_uncast_exception(self, obj):
        raise DbxIOTypeError(f'Passed uncastable value. Cannot cast `{obj}` to type {self}')

    def try_fit(self, obj, unsafe: bool = False):
        if not unsafe and not self.fit(obj):
            self.raise_uncast_exception(obj)


class TinyIntType(BaseType):
    """
    Represents 1-byte signed integer numbers.
    The range of numbers is from -128 to 127.

    Same as numpy.int8
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, np.integer):
            obj = int(obj)
        if not isinstance(obj, int):
            return False
        if -(2**7) <= int(obj) <= 2**7 - 1:
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'{int(obj)}Y'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('Y'):
            return int(obj.strip('Y'))
        return int(obj)


class SmallIntType(BaseType):
    """
    Represents 2-byte signed integer numbers.
    The range of numbers is from -32,768 to 32,767.

    Same as numpy.int16
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, np.integer):
            obj = int(obj)
        if not isinstance(obj, int):
            return False
        if -(2**15) <= obj <= 2**15 - 1:
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'{int(obj)}S'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('S'):
            return int(obj.strip('S'))
        return int(obj)


class IntType(BaseType):
    """
    Represents 4-byte signed integer numbers.
    The range of numbers is from -2,147,483,648 to 2,147,483,647.

    Same as numpy.int32
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, np.integer):
            obj = int(obj)
        if isinstance(obj, int) and -(2**31) <= obj <= 2**31 - 1:
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'{int(obj)}'

    @nullable
    def deserialize(self, obj):
        return int(obj)


class BigIntType(BaseType):
    """
    Represents 8-byte signed integer numbers.
    The range of numbers is from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

    Same as numpy.int64
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, np.integer):
            obj = int(obj)
        if isinstance(obj, int) and -(2**63) <= obj <= 2**63 - 1:
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'{int(obj)}L'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('L'):
            return int(obj.strip('L'))
        return int(obj)


class DecimalType(BaseType):
    """
    Represents numbers with a specified maximum precision and fixed scale.
    """

    def __init__(self, precision: int = 10, scale: int = 0):
        assert 1 <= precision <= 38, 'Precision must be between 1 and 38'
        assert 0 <= scale <= precision, 'Scale must be between 0 and precision'

        self.precision = precision
        self.scale = scale

    def __str__(self):
        return f'DECIMAL({self.precision},{self.scale})'

    def fit(self, obj) -> bool:
        if isinstance(obj, (int, float, Decimal)):
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'{str(obj)}BD'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('BD'):
            return float(obj.strip('BD'))
        return float(obj)


class FloatType(BaseType):
    """
    Represents 8-byte double-precision floating point numbers.
    The range of numbers is:
     > Negative infinity
     > -3.402E+38 to -1.175E-37
     > 0
     > +1.175E-37 to +3.402E+38
     > Positive infinity
     > NaN (not a number)

    Same as numpy.float32
    """

    def fit(self, obj) -> bool:
        if not isinstance(obj, (int, float, np.floating)):
            return False
        if isinstance(obj, np.floating):
            obj = float(obj)
        if obj in (float('inf'), -float('inf')):
            return True
        if -3.402e38 <= obj <= -1.175e-37:
            return True
        if +1.175e-37 <= obj <= +3.402e38:
            return True
        if obj == 0.0 or obj is None:
            return True
        if math.isnan(obj):
            return True

        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        if obj == float('inf'):
            return "float('inf')"
        if obj == -float('inf'):
            return "float('-inf')"
        return f'{str(obj)}F'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('F'):
            return float(obj.strip('F'))
        if isinstance(obj, str) and 'inf' in obj:
            if obj == "float('inf')":
                return float('inf')
            if obj == "float('-inf')":
                return -float('inf')
        return float(obj)


class DoubleType(BaseType):
    """
    Represents 8-byte double-precision floating point numbers.
    The range of numbers is:
     > Negative infinity
     > -1.79769E+308 to -2.225E-307
     > 0
     > +2.225E-307 to +1.79769E+308
     > Positive infinity
     > NaN (not a number)

    Same as numpy.float64
    """

    def fit(self, obj) -> bool:
        if not isinstance(obj, (int, float, np.floating)):
            return False
        if isinstance(obj, np.floating):
            obj = float(obj)
        if obj in (float('inf'), -float('inf')):
            return True
        if -1.79769e308 <= obj <= -2.225e-307:
            return True
        if +2.225e-307 <= obj <= +1.79769e308:
            return True
        if obj == 0.0 or obj is None:
            return True
        if math.isnan(obj):
            return True

        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        if obj == float('inf'):
            return "double('inf')"
        if obj == -float('inf'):
            return "double('-inf')"
        return f'{str(obj)}D'

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str) and obj.endswith('D'):
            return float(obj.strip('D'))
        if isinstance(obj, str) and 'inf' in obj:
            if obj == "double('inf')":
                return float('inf')
            if obj == "double('-inf')":
                return -float('inf')
        return float(obj)


class DateType(BaseType):
    """
    Represents values comprising values of fields year, month, and day, without a time-zone.
    The range of dates supported is June 23 -5877641 CE to July 11 +5881580 CE.
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, datetime.date):
            return True
        elif isinstance(obj, str):
            try:
                dtm = datetime.datetime.strptime(obj, '%Y-%m-%d')
                if dtm.time() == datetime.time(0, 0):
                    return True
                else:
                    return False
            except ValueError:
                return False
        else:
            return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f"DATE'{obj}'" if isinstance(obj, str) else f"DATE'{obj.isoformat()}'"

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, datetime.date):
            return obj
        if isinstance(obj, str) and obj.startswith("DATE'"):
            return datetime.date.fromisoformat(obj.strip("DATE''"))
        return datetime.datetime.strptime(obj, '%Y-%m-%d')


class TimestampType(BaseType):
    """
    Represents values comprising values of fields year, month, day, hour, minute, and second,
    with the session local time-zone. The timestamp value represents an absolute point in time.
    The range of timestamps supported is June 23 -5877641 CE to July 11 +5881580 CE.
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return True
        elif isinstance(obj, str):
            try:
                dateutil.parser.parse(obj)
                return True
            except dateutil.parser.ParserError:
                return False
        else:
            return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f"TIMESTAMP'{obj}'" if isinstance(obj, str) else f"TIMESTAMP'{obj.isoformat()}'"

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj
        if isinstance(obj, str) and obj.startswith("TIMESTAMP'"):
            return datetime.datetime.fromisoformat(obj.strip("TIMESTAMP''"))
        return dateutil.parser.parse(obj)


def do_brackets_sanity_check(s: str) -> bool:
    brackets_pairs = {'(': ')', '[': ']', '{': '}'}
    open_brackets = brackets_pairs.keys()
    close_brackets = brackets_pairs.values()
    stack = []
    for c in s:
        if c in open_brackets:
            stack.append(c)
        elif c in close_brackets:
            if not stack:
                return False
            if brackets_pairs[stack.pop()] != c:
                return False
    return not stack


class ArrayType(BaseType):
    """
    Represents values comprising a sequence of elements with the type of elementType.
    The array type supports sequences of any length greater or equal to 0.
    """

    def __init__(self, element_type: 'BaseType'):
        self.element_type = element_type
        self._is_element_type_complex = element_type.__class__ in ComplexDataTypes

    def __str__(self):
        return f'ARRAY<{self.element_type}>'

    def fit(self, obj) -> bool:
        if isinstance(obj, list) and all([self.element_type.fit(val) for val in obj]):
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'ARRAY({", ".join(map(str, [self.element_type.serialize(val, unsafe=True) for val in obj]))})'

    @staticmethod
    def _split_first_layer(serialized_array: str):
        depth = 0
        current_element_start_idx = None

        for i in range(len(serialized_array)):
            if serialized_array[i] == '(':
                depth += 1
                if current_element_start_idx is None:
                    current_element_start_idx = i + 1
            elif serialized_array[i] == ')':
                depth -= 1
                if depth == 1:
                    current_element_end_idx = i + 1
                    yield serialized_array[current_element_start_idx:current_element_end_idx]
                    current_element_start_idx = None
            elif serialized_array[i] in (',', ' '):
                if depth == 1:
                    current_element_start_idx = i + 1

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, np.ndarray):
            return [self.element_type.deserialize(val) for val in obj.tolist()]
        if isinstance(obj, list):
            return [self.element_type.deserialize(val) for val in obj]
        if isinstance(obj, str) and obj.startswith('ARRAY(') and obj.endswith(')') and do_brackets_sanity_check(obj):
            if obj == 'ARRAY()':
                return []
            if not self._is_element_type_complex:
                return [self.element_type.deserialize(val.strip()) for val in obj.strip('ARRAY()').split(',')]

            return [self.element_type.deserialize(val.strip()) for val in self._split_first_layer(obj)]

        super().raise_uncast_exception(obj)


class BinaryType(BaseType):
    """
    Represents byte sequence values.
    The type supports byte sequences of any length greater or equal to 0.
    """

    def fit(self, obj) -> bool:
        if isinstance(obj, (str, bytes)):
            return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f"X'{obj.encode().hex().upper()}'" if isinstance(obj, str) else f"X'{obj.hex().upper()}'"

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str):
            # At first need to remove support chars. Pattern is X'<hex_string>'
            return bytearray.fromhex(obj.replace("'", '')[1:])
        return obj


class BooleanType(BaseType):
    """
    Represents Boolean values.
    The type supports true and false values.
    """

    def fit(self, obj) -> bool:
        return isinstance(obj, (bool, np.bool_))

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return str(bool(obj)).lower()

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, bool):
            return obj
        if isinstance(obj, str):
            if obj.lower() == 'true':
                return True
            if obj.lower() == 'false':
                return False
        return obj


class IntervalType(BaseType):
    """
    Represents intervals of time either on a scale of seconds or months.
    A year-month interval has a maximal range of +/- 178,956,970 years and 11 months.
    A day-time interval has a maximal range of +/- 106,751,991 days, 23 hours, 59 minutes, and 59.999999 seconds.
    """

    def fit(self, obj) -> bool:
        logger.warning('INTERVAL type is not yet fully supported. Will be used as-is!')
        try:
            str(obj)
            return True
        except ValueError:
            return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return f'INTERVAL {obj}'

    @nullable
    def deserialize(self, obj):
        return obj


class MapType(BaseType):
    """
    Represents values comprising a set of key-value pairs.
    The map type supports maps of any cardinality greater or equal to 0.
    The keys must be unique and not be NULL.
    MAP is not a comparable data type.
    """

    def __init__(self, key_type: 'BaseType', value_type: 'BaseType'):
        self.key_type = key_type
        self.value_type = value_type

    def __str__(self):
        return f'MAP<{self.key_type}, {self.value_type}>'

    def fit(self, obj) -> bool:
        if isinstance(obj, dict):
            pairs = obj.items()
            if all([self.key_type.fit(key) and self.value_type.fit(val) for key, val in pairs]):
                return True
        return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        if isinstance(obj, dict):
            obj = tuple([item for pair in obj.items() for item in pair])
        return f'MAP{obj}'

    @nullable
    def deserialize(self, obj):
        logger.warning('MAP type with complex inner types is not supported for deserialization. Will be used as-is!')
        if isinstance(obj, str) and obj.startswith('MAP'):
            arr = list(map(json.loads, obj.strip('MAP()').split(', ')))
            if len(arr) % 2:
                raise DbxIOTypeError(f'Passed map with odd number of elements, got {obj}')
            return dict(zip(arr[::2], arr[1::2]))
        return dict(obj)


class StringType(BaseType):
    """
    The type supports character sequences of any length greater or equal to 0.
    """

    def fit(self, obj) -> bool:
        return isinstance(obj, str)

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        str_obj = json.dumps(str(obj))
        return f'{str_obj}'

    @nullable
    def deserialize(self, obj):
        return str(obj).replace('"', '')


class StructType(BaseType):
    """
    Represents values with the structure described by a sequence of fields.
    The type supports any number of fields greater or equal to 0.
    """

    def __init__(self, fields: list[tuple[str, 'BaseType']]):
        self.fields = fields

    def __str__(self):
        return f'STRUCT<{", ".join([f"{name} {type_}" for name, type_ in self.fields])}>'

    def fit(self, obj) -> bool:
        if not isinstance(obj, dict):
            return False
        if len(obj) != len(self.fields):
            return False
        for name, type_ in self.fields:
            if name not in obj:
                return False
            if not type_.fit(obj[name]):
                return False
        return True

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        sql_named_struct_values = []
        for name, type_ in self.fields:
            sql_named_struct_values.append(f'"{name}"')
            sql_named_struct_values.append(str(type_.serialize(obj[name], unsafe=True)))

        return f'named_struct({",".join(sql_named_struct_values)})'

    @nullable
    def deserialize(self, obj):
        return obj


class NullType(BaseType):
    def fit(self, obj) -> bool:
        return obj is None

    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return 'NULL'

    def deserialize(self, obj):
        return None


class JSONType(BaseType):
    def __str__(self):
        return str(StringType())

    def fit(self, obj) -> bool:
        try:
            json.dumps(obj)
            return True
        except TypeError:
            return False

    @nullable
    def serialize(self, obj, unsafe: bool = False):
        self.try_fit(obj, unsafe)
        return json.dumps(json.dumps(obj))

    @nullable
    def deserialize(self, obj):
        if isinstance(obj, str):
            return json.loads(obj)
        if isinstance(obj, dict):
            return obj
        raise DbxIOTypeError(f'Cannot cast value `{obj}` to JSON')


class GroupsPrimaryDataTypes:
    INTEGER = (IntType, BigIntType, DecimalType)
    FLOAT = (FloatType, DoubleType)
    DATETIME = (DateType, TimestampType)


ComplexDataTypes = (ArrayType, MapType, StructType)


def as_dbxio_type(value: Any) -> BaseType:
    if isinstance(value, bool) and BooleanType().fit(value):
        return BooleanType()
    if isinstance(value, (int, np.integer)):
        for int_type in GroupsPrimaryDataTypes.INTEGER:
            type_ = int_type()
            if type_.fit(value):
                return type_
    if isinstance(value, (int, float, np.floating)):
        for float_type in GroupsPrimaryDataTypes.FLOAT:
            if float_type().fit(value):
                return float_type()
    if isinstance(value, bytes) and BinaryType().fit(value):
        return BinaryType()
    if isinstance(value, datetime.datetime):
        for dtm_type in GroupsPrimaryDataTypes.DATETIME:
            if dtm_type().fit(value):
                return dtm_type()
    if isinstance(value, list):
        value_type = as_dbxio_type(value[0])
        if ArrayType(value_type).fit(value):
            return ArrayType(value_type)
    if isinstance(value, dict):
        key_type = as_dbxio_type(next(iter(value.keys())))
        value_type = as_dbxio_type(next(iter(value.values())))
        if MapType(key_type=key_type, value_type=value_type).fit(value):
            return MapType(key_type=key_type, value_type=value_type)
    if isinstance(value, dict):
        fields = [(key, as_dbxio_type(val)) for key, val in value.items()]
        if StructType(fields).fit(value):
            return StructType(fields)
    if StringType().fit(value):
        return StringType()
    raise DbxIOTypeError(f"Couldn't determine dbxio type for value {value} of type {type(value)}")


dbxio2pa_types_mapping = {
    str(TinyIntType()): pa.int8(),
    str(SmallIntType()): pa.int16(),
    str(IntType()): pa.int32(),
    str(BigIntType()): pa.int64(),
    str(FloatType()): pa.float32(),
    str(DoubleType()): pa.float64(),
    str(DateType()): pa.date32(),
    str(TimestampType()): pa.timestamp('ns'),
    str(BinaryType()): pa.binary(),
    str(BooleanType()): pa.bool_(),
    str(StringType()): pa.string(),
    str(JSONType()): pa.string(),
    str(NullType()): pa.null(),
}


def convert_dbxio_type_to_pa_type(dbxio_type: BaseType) -> pa.DataType:
    if isinstance(dbxio_type, ArrayType):
        return pa.list_(convert_dbxio_type_to_pa_type(dbxio_type.element_type))
    if isinstance(dbxio_type, MapType):
        return pa.map_(
            convert_dbxio_type_to_pa_type(dbxio_type.key_type), convert_dbxio_type_to_pa_type(dbxio_type.value_type)
        )
    if isinstance(dbxio_type, StructType):
        return pa.struct([pa.field(name, convert_dbxio_type_to_pa_type(type_)) for name, type_ in dbxio_type.fields])
    if isinstance(dbxio_type, DecimalType):
        return pa.decimal128(dbxio_type.precision, dbxio_type.scale)

    try:
        return dbxio2pa_types_mapping[str(dbxio_type)]
    except KeyError:
        raise DbxIOTypeError(f'Cannot convert dbxio type {dbxio_type} to pyarrow type')
