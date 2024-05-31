import enum
from typing import Optional, Union

import attrs

from dbxio.delta.table_schema import TableSchema
from dbxio.sql.types import BaseType


class TableType(enum.Enum):
    Managed = 'Managed'
    Unmanaged = 'Unmanaged'


class TableFormat(enum.Enum):
    TEXT = 'TEXT'
    AVRO = 'AVRO'
    BINARYFILE = 'BINARYFILE'
    CSV = 'CSV'
    JSON = 'JSON'
    PARQUET = 'PARQUET'
    ORC = 'ORC'
    DELTA = 'DELTA'
    JDBC = 'JDBC'
    LIBSVM = 'LIBSVM'


class Materialization(enum.Enum):
    Table = 'Table'
    View = 'View'


@attrs.define
class TableAttributes:
    location: Optional[str] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    partitioned_by: Optional[list[str]] = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            attrs.validators.deep_iterable(
                member_validator=attrs.validators.instance_of(str),
                iterable_validator=attrs.validators.instance_of(list),
            )
        ),
    )
    properties: Optional[dict[str, str]] = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(dict)),
    )


def _table_schema_converter(schema: Union[dict[str, BaseType], list[dict[str, BaseType]], TableSchema]) -> TableSchema:
    return TableSchema.from_obj(schema)


def _table_identifier_validator(instance, attribute, value):
    if len(value.split('.')) != 3:
        raise ValueError(
            "It's not allowed to create/or use tables from default "
            f'database (got {value}). Please specify table path in the following format: '
            '<catalog>.<schema>.<table_name>'
        )


@attrs.define(slots=True)
class Table:
    """
    Represents a table in Databricks Unity Catalog. Used to define views as well as tables.
    """

    table_identifier: str = attrs.field(
        validator=[
            attrs.validators.instance_of(str),
            _table_identifier_validator,
        ]
    )
    table_format: TableFormat = attrs.field(
        default=TableFormat.DELTA,
        validator=attrs.validators.instance_of(TableFormat),
    )
    materialization: Materialization = attrs.field(
        default=Materialization.Table,
        validator=attrs.validators.instance_of(Materialization),
    )
    schema: Optional[Union[dict[str, BaseType], list[dict[str, BaseType]], TableSchema]] = attrs.field(
        default=None,
        converter=_table_schema_converter,
    )
    attributes: TableAttributes = attrs.field(factory=TableAttributes)

    @classmethod
    def from_obj(cls, obj: Union[str, 'Table']):
        if isinstance(obj, Table):
            return obj
        else:
            return Table(table_identifier=obj)

    @property
    def safe_table_identifier(self):
        """
        Returns table identifier with special characters replaced with underscores and wrapped in backticks.
        """
        trunc_ti = self.table_identifier.translate(
            str.maketrans('!"#$%&\'()*+,/:;<=>?@[\\]^`{|}~', '_____________________________')
        )
        return '.'.join([f'`{ti_part}`' for ti_part in trunc_ti.split('.')])

    @property
    def is_unmanaged(self) -> bool:
        return bool(self.attributes.location)

    @property
    def full_external_path(self) -> Optional[str]:
        return f'{self.table_format.name}.`{self.attributes.location}`' if self.is_unmanaged else None
