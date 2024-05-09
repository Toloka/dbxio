import enum
from typing import Optional, Union

import attrs

from dbxio.delta.table_schema import TableSchema
from dbxio.delta.types import BaseType


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


def _table_schema_converter(schema: Union[list[dict[str, BaseType]], TableSchema]) -> TableSchema:
    return TableSchema.from_obj(schema)


@attrs.define(slots=True)
class Table:
    """
    Represents a table in Databricks Unity Catalog. Used to define views as well as tables.
    """

    _table_identifier: str = attrs.field(validator=attrs.validators.instance_of(str), alias='table_identifier')
    table_format: TableFormat = attrs.field(
        default=TableFormat.DELTA,
        validator=attrs.validators.instance_of(TableFormat),
    )
    materialization: Materialization = attrs.field(
        default=Materialization.Table,
        validator=attrs.validators.instance_of(Materialization),
    )
    _table_schema: Optional[Union[list[dict[str, BaseType]], TableSchema]] = attrs.field(
        default=None,
        converter=_table_schema_converter,
        alias='schema',
    )
    attributes: TableAttributes = attrs.field(factory=TableAttributes)

    @classmethod
    def from_obj(cls, obj: Union[str, 'Table']):
        if isinstance(obj, Table):
            return obj
        else:
            return Table(table_identifier=obj)

    @property
    def table_identifier(self):
        return self._table_identifier

    @table_identifier.setter
    def table_identifier(self, new_table_identifier: str) -> None:
        assert len(new_table_identifier.split('.')) == 3, (
            "It's not allowed to create/or use tables from default "
            f'database (got {new_table_identifier}). Please specify table path in the following format: '
            '<catalog>.<schema>.<table_name>'
        )

        self._table_identifier = new_table_identifier

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
    def schema(self):
        return self._table_schema

    @schema.setter
    def schema(self, new_schema: Union[list[dict[str, BaseType]], TableSchema]):
        self._table_schema = TableSchema.from_obj(new_schema)

    @property
    def is_unmanaged(self) -> bool:
        return bool(self.attributes.location)

    @property
    def full_external_path(self) -> Optional[str]:
        return f'{self.table_format.name}.`{self.attributes.location}`' if self.is_unmanaged else None
