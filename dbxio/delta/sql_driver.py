import logging
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import attrs
from databricks import sql
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementExecutionAPI,
    StatementParameterListItem,
)
from databricks.sql.client import Connection, Cursor

from dbxio.core.auth import ClusterCredentials
from dbxio.delta.query import QUERY_PARAMS_TYPE, BaseDatabricksQuery, ConstDatabricksQuery, log_query
from dbxio.delta.sql_utils import _FutureBaseResult, _FutureODBCResult, _FutureStatementApiResult
from dbxio.utils.databricks import ClusterType

if TYPE_CHECKING:
    pass


@attrs.define(slots=True)
class SQLDriver:
    """
    Interface for SQL drivers.
    """

    @property
    @abstractmethod
    def cluster_type(self) -> ClusterType:
        raise NotImplementedError

    @property
    @abstractmethod
    def cluster_credentials(self) -> ClusterCredentials:
        raise NotImplementedError

    @abstractmethod
    def _sql_impl(self, built_query: str, built_params: QUERY_PARAMS_TYPE) -> _FutureBaseResult:
        raise NotImplementedError

    @abstractmethod
    def _sql_to_files_impl(
        self,
        built_query: str,
        built_params: QUERY_PARAMS_TYPE,
        results_path: str,
        max_concurrency: int = 1,
    ) -> Path:
        raise NotImplementedError

    def _build_query(self, query: Union[str, BaseDatabricksQuery]) -> tuple[str, QUERY_PARAMS_TYPE]:
        if isinstance(query, str):
            query = ConstDatabricksQuery(query)
        built_query, built_params = query.build(self.cluster_type)
        log_query(built_query)

        return built_query, built_params

    def sql(self, query: Union[str, BaseDatabricksQuery]) -> _FutureBaseResult:
        """
        Execute the SQL query and return the future with the results.
        """
        built_query, built_params = self._build_query(query)

        return self._sql_impl(built_query=built_query, built_params=built_params)

    def sql_to_files(
        self,
        query: Union[str, BaseDatabricksQuery],
        results_path: str,
        max_concurrency: int = 1,
    ) -> Path:
        """
        Execute the SQL query and save the results to the specified directory.
        Returns the path to the directory with the results including the statement ID.
        If statement ID is not provided by Databricks, it will be generated.
        """
        assert max_concurrency > 0, 'max_concurrency must be greater than 0'
        if results_path:
            assert Path(results_path).is_dir(), f'{results_path} is not a valid directory'

        built_query, built_params = self._build_query(query)

        return self._sql_to_files_impl(
            built_query=built_query,
            built_params=built_params,
            results_path=results_path,
            max_concurrency=max_concurrency,
        )


@attrs.define(slots=True)
class ODBCDriver(SQLDriver):
    """
    SQL driver for all-purpose clusters.

    Warning: ODBC connection can be extremely slow.
    Consider using the SQL Warehouse cluster type instead.

    Docs: https://docs.databricks.com/en/dev-tools/python-sql-connector.html
    """

    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))
    cluster_credentials: ClusterCredentials = attrs.field(validator=attrs.validators.instance_of(ClusterCredentials))
    session_configuration: Optional[dict[str, Any]] = attrs.field(default=None)

    conn: Optional[Connection] = attrs.field(default=None, init=False, repr=False)
    cursor: Optional[Cursor] = attrs.field(default=None, init=False, repr=False)
    thread_pool: ThreadPoolExecutor = attrs.Factory(ThreadPoolExecutor)

    def as_dict(self) -> dict[str, Any]:
        return {
            'server_hostname': self.cluster_credentials.server_hostname,
            'http_path': self.cluster_credentials.http_path,
            'access_token': self.cluster_credentials.access_token,
            'session_configuration': self.session_configuration,
        }

    def _sql_impl(self, built_query: str, built_params: QUERY_PARAMS_TYPE) -> _FutureODBCResult:
        logging.warning(
            'You are using the ODBC connection to execute the query, because the cluster type is ALL_PURPOSE. '
            'ODBC connection is very slow. Consider using the SQL Warehouse cluster type instead.',
        )

        def _execute_sql_query():
            if not self.conn or not self.cursor or not self.cursor.open or not self.conn.open:
                self.conn = sql.connect(**self.as_dict())
                self.cursor = self.conn.cursor()

            self.cursor.execute(operation=built_query, parameters=built_params)
            return self.conn, self.cursor

        return _FutureODBCResult(self.thread_pool.submit(_execute_sql_query))

    def _sql_to_files_impl(
        self,
        built_query: str,
        built_params: QUERY_PARAMS_TYPE,
        results_path: str,
        max_concurrency: int = 1,
    ) -> Path:
        if max_concurrency > 1:
            logging.warning('Concurrent downloads of SQL results are not supported for ALL_PURPOSE clusters.')

        future = self._sql_impl(built_query, built_params)
        return future.download_and_save(results_path=results_path)


@attrs.define(slots=True)
class StatementAPIDriver(SQLDriver):
    """
    SQL driver for SQL warehouses.

    Docs: https://docs.databricks.com/en/dev-tools/sql-execution-tutorial.html
    """

    cluster_type: ClusterType = attrs.field(validator=attrs.validators.instance_of(ClusterType))
    cluster_credentials: ClusterCredentials = attrs.field(validator=attrs.validators.instance_of(ClusterCredentials))
    statement_api: StatementExecutionAPI = attrs.field(validator=attrs.validators.instance_of(StatementExecutionAPI))

    def _sql_impl(self, built_query: str, built_params: QUERY_PARAMS_TYPE) -> _FutureStatementApiResult:
        warehouse_id = self.cluster_credentials.http_path.split('/')[-1]
        assert warehouse_id, f'Warehouse ID is not found in the HTTP path, got {self.cluster_credentials.http_path}'
        if built_params:
            assert all(
                isinstance(p, StatementParameterListItem) for p in built_params
            ), f'Invalid parameters types, got {built_params}'

        statement_response = self.statement_api.execute_statement(
            statement=built_query,
            parameters=built_params,  # type: ignore
            warehouse_id=warehouse_id,
            disposition=Disposition.EXTERNAL_LINKS,
            format=Format.ARROW_STREAM,
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )
        return _FutureStatementApiResult(statement_response, statement_api=self.statement_api)

    def _sql_to_files_impl(
        self,
        built_query: str,
        built_params: QUERY_PARAMS_TYPE,
        results_path: str,
        max_concurrency: int = 1,
    ) -> Path:
        result = self._sql_impl(built_query, built_params)
        return result.download_and_save(results_path=results_path, max_concurrency=max_concurrency)


def get_sql_driver(
    cluster_type: ClusterType,
    cluster_credentials: ClusterCredentials,
    statement_api: StatementExecutionAPI,
    session_configuration: Optional[dict[str, Any]] = None,
) -> SQLDriver:
    """
    Factory function to create the SQL driver based on the cluster type.
    """
    if cluster_type is ClusterType.ALL_PURPOSE:
        return ODBCDriver(
            cluster_type=cluster_type,
            cluster_credentials=cluster_credentials,
            session_configuration=session_configuration,
        )
    elif cluster_type is ClusterType.SQL_WAREHOUSE:
        return StatementAPIDriver(
            cluster_type=cluster_type,
            cluster_credentials=cluster_credentials,
            statement_api=statement_api,
        )
    else:
        raise ValueError(f'Unsupported cluster type: {cluster_type}')
