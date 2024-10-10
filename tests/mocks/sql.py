from dbxio.sql.query import BaseDatabricksQuery


def flatten_query(query):
    if isinstance(query, BaseDatabricksQuery):
        query = query.query
    return ' '.join([s.strip() for s in query.splitlines()]).strip()


class ArbitraryRecord(dict):
    def __getitem__(self, key):
        if key not in self:
            return f'value_{key}'
        return super().__getitem__(key)


def sql_mock(query):
    """
    Returns context manager that returns one test record on __enter__ call
    """

    class MockSql:
        def __enter__(self):
            return [ArbitraryRecord(a=1, b=2)]

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def wait(self):
            pass

    return MockSql()
