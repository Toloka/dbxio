import json
import logging
import warnings
from abc import ABCMeta, abstractmethod
from typing import Any, Union

import attrs
import jinja2
from databricks.sdk.service.sql import (
    StatementParameterListItem,
)

from dbxio.delta.types import as_dbxio_type
from dbxio.utils.databricks import ClusterType

QUERY_PARAMS_TYPE = Union[dict[str, Any], list[StatementParameterListItem], None]


def log_query(built_query: str) -> None:
    logging.info('Executing query:\n%s', built_query)


def cast_params_to_statement_api(params: Union[dict, None]) -> Union[list[StatementParameterListItem], None]:
    if params is None:
        return None

    new_params = []
    for key, value in params.items():
        dbxio_type = as_dbxio_type(value)
        new_params.append(StatementParameterListItem(name=key, value=value, type=str(dbxio_type)))

    return new_params


class BaseDatabricksQuery(metaclass=ABCMeta):
    @abstractmethod
    def build(self, cluster_type: ClusterType) -> tuple[str, QUERY_PARAMS_TYPE]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def params(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def query(self) -> str:
        raise NotImplementedError()


@attrs.define
class ConstDatabricksQuery(BaseDatabricksQuery):
    query: str
    params = None

    def build(self, cluster_type: ClusterType) -> tuple[str, None]:
        return self.query, None


@attrs.define
class JinjaDatabricksQuery(BaseDatabricksQuery):
    query: str
    params: dict[str, Any]

    def __attrs_post_init__(self):
        warnings.warn(
            'JinjaDatabricksQuery is deprecated and will be removed in the future. '
            'Use ParametrizedDatabricksQuery instead',
            DeprecationWarning,
        )

    def build(self, cluster_type: ClusterType) -> tuple[str, dict[str, Any]]:
        jinja_env = jinja2.Environment()
        query_template = jinja_env.from_string(self.query)
        for k, v in self.params.items():
            if isinstance(v, (list, tuple, set)):
                self.params[k] = f"({','.join(map(json.dumps, v))})"
        return query_template.render(**self.params), self.params


@attrs.define
class ParametrizedDatabricksQuery(BaseDatabricksQuery):
    """
    A class for parametrized Databricks SQL queries.
    Only primitive types are supported as parameters.
    Lists, tuples, and sets are converted to comma-separated strings.
    """

    query: str
    params: dict[str, Any]

    def build(self, cluster_type: ClusterType) -> tuple[str, QUERY_PARAMS_TYPE]:
        built_query = self.query

        if cluster_type == ClusterType.SQL_WAREHOUSE:
            return built_query, cast_params_to_statement_api(self.params)

        return built_query, self.params
