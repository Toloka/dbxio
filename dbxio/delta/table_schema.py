from functools import cache
from typing import Any

try:
    import pydantic.v1 as pydantic
except ModuleNotFoundError:
    import pydantic  # type: ignore

from dbxio.sql.types import BaseType


class ColumnSpec(pydantic.BaseModel):
    name: str
    type: BaseType

    class Config:
        arbitrary_types_allowed = True


class TableSchema:
    def __init__(self, raw_schema: list[dict[str, BaseType]]):
        self._columns: list[ColumnSpec] = self._check_schema_obj(raw_schema)

    def __getattr__(self, item):
        try:
            column = [col for col in self._columns if col.name == item][0]
        except IndexError:
            raise AttributeError(f'Column {item} not found in schema. Possible columns: {self.columns}')
        return column

    @classmethod
    def from_obj(cls, obj):
        if obj is None:
            return None
        if isinstance(obj, list):
            return TableSchema(obj)
        if isinstance(obj, dict):
            return TableSchema([{'name': k, 'type': v} for k, v in obj.items()])
        elif isinstance(obj, TableSchema):
            return obj
        else:
            raise TypeError(f'Got unexpected obj with type {type(obj)}')

    @staticmethod
    def _check_schema_obj(schema: list[dict]) -> list[ColumnSpec]:
        return pydantic.parse_obj_as(list[ColumnSpec], schema)

    @property
    def columns(self):
        return [col.name for col in self._columns]

    @cache
    def as_dict(self) -> dict[str, BaseType]:
        return {col_spec.name: col_spec.type for col_spec in self._columns}

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        return {key: self.as_dict()[key].deserialize(val) for key, val in record.items()}
