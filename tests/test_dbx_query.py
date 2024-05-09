from databricks.sdk.service.sql import StatementParameterListItem

from dbxio.delta.query import ParametrizedDatabricksQuery
from dbxio.utils.databricks import ClusterType


def test_parametrized_databricks_query():
    query = ParametrizedDatabricksQuery(
        query='SELECT * FROM table WHERE column = :value',
        params={'value': 'value'},  # noqa
    )
    built_query, built_params = query.build(ClusterType.SQL_WAREHOUSE)
    assert built_params == [StatementParameterListItem(name='value', value='value', type='STRING')]
