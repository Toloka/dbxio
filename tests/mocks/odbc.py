from typing import Optional

import numpy as np
import pyarrow as pa
from databricks.sql.types import Row

MOCK_ROW = Row(
    col_int=1,
    col_string='foobar',
    col_boolean=True,
    col_array=np.array([1, -1, 42]),
    col_map={'a': 1},
    col_json='{"k1": "v1", "k2": true, "k3": 1.1, "k4": 42}',
)
TOTAL_MOCK_RECORDS = 100


class MockCursor:
    def __init__(self, total_records=TOTAL_MOCK_RECORDS, *args, **kwargs):
        self.one_row = MOCK_ROW
        self.total_records = total_records
        self.current_index = 0

        self._rows_offset = 0

        self.open = True

    def close(self):
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def execute(self, operation: str, parameters: Optional[dict[str, str]] = None):
        return self

    def fetchone(self) -> Optional[Row]:
        record = self.one_row
        if self.current_index < self.total_records:
            self.current_index += 1
            return record
        else:
            return None

    def fetchmany(self, size: int) -> list[Row]:
        return [self.one_row for _ in range(size)]

    def fetchall(self) -> list[Row]:
        return self.fetchmany(self.total_records)

    def fetchmany_arrow(self, size: int):
        if self._rows_offset + size <= self.total_records:
            rows_to_fetch = size
            self._rows_offset += size
        elif self._rows_offset < self.total_records:
            rows_to_fetch = self.total_records - self._rows_offset
            self._rows_offset = self.total_records
        else:
            rows_to_fetch = 0
        return pa.Table.from_pylist([r.asDict() for r in self.fetchmany(rows_to_fetch)])


def mock_dbx_connect(server_hostname, http_path, access_token, **kwargs):
    return MockConnection(server_hostname, http_path, access_token, **kwargs)


class MockConnection:
    DEFAULT_RESULT_BUFFER_SIZE_BYTES = 10485760
    DEFAULT_ARRAY_SIZE = 100000

    def __init__(self, server_hostname, http_path, access_token, **kwargs):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token

        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def cursor(
        self,
        arraysize: int = DEFAULT_ARRAY_SIZE,
        buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
    ):
        return MockCursor()

    def close(self):
        pass
