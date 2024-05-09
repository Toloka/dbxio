import concurrent.futures
import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Optional, Union

import pyarrow as pa
from databricks.sdk.service.sql import (
    ExecuteStatementResponse,
    StatementExecutionAPI,
    StatementState,
    StatementStatus,
)
from databricks.sql.client import Connection, Cursor

from dbxio.blobs.parquet import arrow_stream2parquet, pa_table2parquet
from dbxio.core.exceptions import ReadDataError
from dbxio.utils.http import get_session

STATEMENT_API_TIMEOUT = 3 * 60 * 60  # 3 hours
SLEEP_TIME = 10

ODBC_BATCH_SIZE_TO_FETCH = 100000


class _FutureBaseResult(metaclass=ABCMeta):
    @abstractmethod
    def __enter__(self):
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError()

    @abstractmethod
    def __next__(self) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def wait(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def full_results_path(self, results_path: Path) -> Union[Path, None]:
        raise NotImplementedError()

    @abstractmethod
    def download_and_save(self, results_path: str, max_concurrency: int = 1) -> Path:
        raise NotImplementedError()


class _FutureODBCResult(_FutureBaseResult):
    def __init__(self, future: concurrent.futures.Future):
        self._future = future

        self.statement_id = str(uuid.uuid4())
        logging.info('Statement ID: %s', self.statement_id)

        self._cached_batch: Optional[list[dict]] = None
        self._cached_batch_idx = 0

        self._batch_index = 0

    def full_results_path(self, results_path: Path) -> Path:
        return results_path / self.statement_id

    def download_and_save(self, results_path: str, max_concurrency: int = 1) -> Path:
        path = Path(results_path)
        assert path.exists(), f'{results_path} does not exist'

        self.wait()

        data_save_path = self.full_results_path(path)
        data_save_path.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                table = self._fetch_batch()
                with open(data_save_path / f'{self._batch_index}.parquet', 'wb') as f:
                    f.write(pa_table2parquet(table))

                self._batch_index += 1
            except StopIteration:
                break

        return data_save_path

    @cached_property
    def _result(self) -> tuple[Connection, Cursor]:
        return list(concurrent.futures.as_completed([self._future]))[0].result()

    @cached_property
    def conn(self) -> Connection:
        return self._result[0]

    @cached_property
    def cursor(self) -> Cursor:
        return self._result[1]

    def __iter__(self):
        concurrent.futures.wait([self._future])
        return self

    def _fetch_batch(self) -> pa.Table:
        table = self.cursor.fetchmany_arrow(ODBC_BATCH_SIZE_TO_FETCH)
        if table.num_rows > 0:
            return table

        raise StopIteration

    def __next__(self) -> dict:
        if self._cached_batch and self._cached_batch_idx < len(self._cached_batch):
            record = self._cached_batch[self._cached_batch_idx]
            self._cached_batch_idx += 1
            return record
        else:
            self._cached_batch = self._fetch_batch().to_pandas().to_dict('records')
            self._cached_batch_idx = 0
            return self.__next__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def wait(self) -> None:
        self._future.result()


class _FutureStatementApiResult(_FutureBaseResult):
    def __init__(self, response: ExecuteStatementResponse, statement_api: StatementExecutionAPI):
        self.statement_api = statement_api
        self._requests_session = get_session()

        assert response.statement_id, 'Statement ID is not found in the response'
        self.statement_id = response.statement_id

        self._cached_batch: list[dict] = []
        self._cached_batch_idx = 0

        self._total_chunk_count: int = (response.manifest.total_chunk_count if response.manifest else 0) or 0
        self._chunk_ptr = 0

    def full_results_path(self, results_path: Path) -> Path:
        return results_path / self.statement_id

    def download_and_save(self, results_path: str, max_concurrency: int = 1) -> Path:
        path = Path(results_path)
        assert path.exists(), f'{results_path} does not exist'
        self.wait()

        data_save_path = self.full_results_path(path)
        data_save_path.mkdir(parents=True, exist_ok=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            executor.map(
                lambda args: self._download_chunk(*args),
                [(chunk_idx, data_save_path) for chunk_idx in range(self._total_chunk_count)],
            )

        self._chunk_ptr = self._total_chunk_count  # to avoid downloading chunks again

        return data_save_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def raise_for_status(self, status: StatementStatus = None):
        if status is None:
            status = self.statement_api.get_statement(self.statement_id).status
            assert status, 'Statement status is not found'
        state = status.state
        assert state, 'Statement state is not found'

        if state == StatementState.CANCELED:
            msg = f'Statement {self.statement_id} was canceled'
            logging.warning(msg)
            raise ReadDataError(msg)

        if state == StatementState.CLOSED:
            msg = f'Statement {self.statement_id} was closed. result no longer available for fetch'
            logging.warning(msg)
            raise ReadDataError(msg)

        if state == StatementState.FAILED:
            msg = f'Statement {self.statement_id} failed. ' f'Error ({status.error.error_code}): {status.error.message}'  # type: ignore
            logging.error(msg)
            raise ReadDataError(msg)

    def _is_statement_ready_to_fetch(self):
        status: StatementStatus = self.statement_api.get_statement(self.statement_id).status
        state: StatementState = status.state
        self.raise_for_status(status)

        if state in (StatementState.PENDING, StatementState.RUNNING):
            return False

        if state == StatementState.SUCCEEDED:
            return True

        raise ValueError(f'Unexpected statement state: {state}')

    def __iter__(self):
        self.wait()

        return self

    def _download_chunk(self, chunk_index, chunk_save_dir: Path = None) -> Union[bytes, Path]:
        chunk_description = self.statement_api.get_statement_result_chunk_n(
            statement_id=self.statement_id,
            chunk_index=chunk_index,
        )
        assert chunk_description.external_links, 'External links are not found in the chunk description'
        chunk = self._requests_session.get(chunk_description.external_links[0].external_link)
        chunk.raise_for_status()
        if not chunk_save_dir:
            return chunk.content

        save_path = chunk_save_dir / f'{chunk_index}.parquet'
        with open(save_path, 'wb') as f:
            f.write(arrow_stream2parquet(chunk.content))
        return save_path

    def __next__(self) -> dict:
        if self._chunk_ptr < self._total_chunk_count:
            if self._cached_batch and self._cached_batch_idx < len(self._cached_batch):
                record = self._cached_batch[self._cached_batch_idx]
                self._cached_batch_idx += 1
                return record
            else:
                chunk_bytes = self._download_chunk(chunk_index=self._chunk_ptr)
                self._cached_batch = pa.ipc.open_stream(chunk_bytes).read_pandas().to_dict('records')
                self._chunk_ptr += 1
                self._cached_batch_idx = 0
                return self.__next__()
        if self._cached_batch_idx < len(self._cached_batch):
            record = self._cached_batch[self._cached_batch_idx]
            self._cached_batch_idx += 1
            return record

        raise StopIteration

    def wait(self, timeout: int = STATEMENT_API_TIMEOUT) -> None:
        """
        Polls the statement status until it's ready to fetch the results.
        :param timeout: timeout in seconds
        """
        poll_start_time = time.time()
        while not self._is_statement_ready_to_fetch():
            logging.info(
                'Statement %s is not ready to fetch. Waiting for %s seconds',
                self.statement_id,
                SLEEP_TIME,
            )
            if time.time() - poll_start_time > timeout:
                raise ReadDataError(f'Statement {self.statement_id} is not ready to fetch. Timeout')

            time.sleep(SLEEP_TIME)

        logging.info('Statement %s is ready to fetch', self.statement_id)
