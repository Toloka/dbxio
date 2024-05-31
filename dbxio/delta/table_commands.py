import logging
import os.path
import uuid
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Union

from databricks.sql import ServerOperationError

from dbxio.blobs.block_upload import upload_file
from dbxio.blobs.parquet import create_pa_table, create_tmp_parquet, pa_table2parquet
from dbxio.delta.parsers import infer_schema
from dbxio.delta.table import Table, TableFormat
from dbxio.sql.query import ConstDatabricksQuery
from dbxio.sql.results import _FutureBaseResult
from dbxio.utils.blobs import blobs_registries, get_blob_servie_client
from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core import DbxIOClient

logger = get_logger()


def exists_table(table: Union[str, Table], client: 'DbxIOClient') -> bool:
    """
    Checks if table exists in the catalog. Tries to read one record from the table.
    """
    table = Table.from_obj(table)

    try:
        next(read_table(table, limit_records=1, client=client))
        return True
    except ServerOperationError:
        return False


def create_table(table: Union[str, Table], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Creates a table in the catalog.
    If a table already exists, it does nothing.
    Query pattern:
        CREATE TABLE IF NOT EXISTS <table_identifier> (col1 type1, col2 type2, ...)
        [USING <table_format> LOCATION <location>]
        [PARTITIONED BY (col1, col2, ...)]
    """
    dbxio_table = Table.from_obj(table)

    schema_sql = ','.join([f'`{col_name}` {col_type}' for col_name, col_type in dbxio_table.schema.as_dict().items()])
    query = f'CREATE TABLE IF NOT EXISTS {dbxio_table.safe_table_identifier} ({schema_sql})'
    if loc := dbxio_table.attributes.location:
        query += f" USING {dbxio_table.table_format.name} LOCATION '{loc}'"
    if part := dbxio_table.attributes.partitioned_by:
        query += f" PARTITIONED BY ({','.join(part)})"

    return client.sql(query)


def drop_table(table: Union[str, Table], client: 'DbxIOClient', force: bool = False) -> _FutureBaseResult:
    """
    Drops a table from the catalog.
    Operation can be forced by setting force=True.
    """
    dbxio_table = Table.from_obj(table)

    force_mark = 'IF EXISTS ' if force else ''
    drop_sql = f'DROP TABLE {force_mark}{dbxio_table.safe_table_identifier}'

    return client.sql(drop_sql)


def read_table(
    table: Union[str, Table],
    client: 'DbxIOClient',
    columns_subset: list[str] = None,
    *,
    distinct: bool = False,
    limit_records: int = None,
) -> Iterator[Dict[str, Any]]:
    """
    Reads data from the table.
    Data amount can be limited by columns_subset, distinct or limit_records options.
    Returns a generator of records and applies schema if it's present.
    """
    logging.getLogger('thrift_backend.py').setLevel('INFO')  # HACK: disable logging from databricks.sql
    dbxio_table = Table.from_obj(table)

    # FIXME use sql-builder
    sql_columns_subset = ','.join([f'`{col}`' for col in columns_subset]) if columns_subset else '*'
    sql_distinct = 'DISTINCT ' if distinct else ''
    _sql_query = f'SELECT {sql_distinct}{sql_columns_subset} FROM {dbxio_table.safe_table_identifier}'
    if limit_records:
        _sql_query += f' LIMIT {limit_records}'

    with client.sql(_sql_query) as fq:
        for records in fq:
            records = records if isinstance(records, list) else [records]
            for record in records:
                if dbxio_table.schema:
                    record = dbxio_table.schema.apply(record)
                yield record


def save_table_to_files(
    table: Union[str, Table],
    client: 'DbxIOClient',
    results_path: str,
    max_concurrency: int = 1,
) -> Path:
    """
    Saves table content to specified path.
    Returns path to the results.

    Data can be saved in multiple files.
    Sql driver determines the number of files.
    """
    dbxio_table = Table.from_obj(table)

    sql_read_query = f'select * from {dbxio_table.safe_table_identifier}'
    return client.sql_to_files(sql_read_query, results_path=results_path, max_concurrency=max_concurrency)


def write_table(
    table: Union[str, Table],
    new_records: Union[Iterator[Dict], List[Dict]],
    client: 'DbxIOClient',
    append: bool = True,
) -> _FutureBaseResult:
    """
    Writes new records to the table using a direct SQL statement.
    Function is relatively fast on small datasets but can be slow or even fail on large datasets.

    For all use cases, consider using bulk_write_table or bulk_write_local_files functions.
    Databricks does not support sql queries larger than 50 Mb.
    """
    dbxio_table = Table.from_obj(table)

    new_records = iter(new_records)
    first_record = next(new_records)

    if dbxio_table.schema is None:
        dbxio_table.schema = infer_schema(first_record)
    schema = dbxio_table.schema.as_dict()

    create_table(dbxio_table, client=client).wait()

    column_names = dbxio_table.schema.columns
    input_way = 'INTO' if append else 'OVERWRITE'
    new_values_sql_format = [
        f'( {",".join([schema[col_name].serialize(first_record.get(col_name)) for col_name in column_names])} )'
    ]
    new_values_sql_format += [
        f'( {",".join([schema[col_name].serialize(record.get(col_name)) for col_name in column_names])} )'
        for record in new_records
    ]

    _sql_query = (
        f"INSERT {input_way} {dbxio_table.safe_table_identifier} ({','.join(column_names)}) "
        f"VALUES {','.join(map(str, new_values_sql_format))}"
    )

    query_size_in_bytes = len(_sql_query.encode('utf-8'))
    if query_size_in_bytes > 10 * 2**20:
        # TODO: raise exception here
        logger.warning(
            f'Query size is {query_size_in_bytes / 2 ** 20} MB. '
            f'Please consider using bulk_write_table function instead of write_table.'
            f'In further versions of dbxio this function will be deprecated and removed.'
        )

    return client.sql(_sql_query)


def copy_into_table(
    client: 'DbxIOClient',
    table: Table,
    blob_path: str,
    table_format: TableFormat,
    abs_name: str,
    abs_container_name: str,
    include_files_pattern: bool = False,
) -> None:
    """
    Copy data from blob storage into the table. All files that match the pattern *.{table_format} will be copied.
    """
    _copy_into_query = dedent(f"""
    COPY INTO {table.safe_table_identifier}
    FROM "abfss://{abs_container_name}@{abs_name}.dfs.core.windows.net/{blob_path}"
    FILEFORMAT = {table_format.value}
    """)
    _pattern = f"PATTERN = '*.{table_format.value.lower()}'" if include_files_pattern else ''
    _options = dedent("""
    FORMAT_OPTIONS ("mergeSchema" = "true")
    COPY_OPTIONS ("mergeSchema" = "true")
    """)

    sql_copy_into_query = ConstDatabricksQuery(f'{_copy_into_query} {_pattern} {_options}')
    client.sql(sql_copy_into_query).wait()


def bulk_write_table(
    table: Union[str, Table],
    new_records: Union[Iterator[Dict], List[Dict]],
    client: 'DbxIOClient',
    abs_name: str,
    abs_container_name: str,
    append: bool = True,
) -> None:
    """
    Bulk write table using parquet format and CLONE INTO statement.
    Function requires blob storage to store temporary parquet file.
    """
    dbxio_table = Table.from_obj(table)
    stream = iter(new_records)
    first_record = next(stream)
    dbxio_table.schema = dbxio_table.schema or infer_schema(first_record)

    columnar_table: dict[str, list] = {col_name: [] for col_name in dbxio_table.schema.columns}
    for col_name in dbxio_table.schema.columns:
        columnar_table[col_name].append(first_record.get(col_name))
    for record in stream:
        for col_name in dbxio_table.schema.columns:
            columnar_table[col_name].append(record.get(col_name))

    pa_table = create_pa_table(columnar_table, schema=dbxio_table.schema)
    pa_table_as_bytes = pa_table2parquet(pa_table)

    abs_client = get_blob_servie_client(
        abs_name,
        az_cred_provider=client.credential_provider.az_cred_provider,
    ).get_container_client(abs_container_name)

    with create_tmp_parquet(pa_table_as_bytes, dbxio_table.table_identifier, abs_client) as tmp_path:
        if not append:
            drop_table(dbxio_table, client=client, force=True).wait()
        create_table(dbxio_table, client=client).wait()

        copy_into_table(
            client=client,
            table=dbxio_table,
            table_format=TableFormat.PARQUET,
            blob_path=tmp_path,
            abs_name=abs_name,
            abs_container_name=abs_container_name,
        )


def bulk_write_local_files(
    table: Table,
    path: str,
    table_format: TableFormat,
    client: 'DbxIOClient',
    abs_name: str,
    abs_container_name: str,
    append: bool = True,
    force: bool = False,
    max_concurrency: int = 1,
) -> None:
    """
    Write data from local files to the table.
    """
    assert table.schema, 'Table schema is required for bulk_write_local_files function'

    abs_client = get_blob_servie_client(abs_name, az_cred_provider=client.credential_provider.az_cred_provider)
    p = Path(path)
    files = p.glob(f'*.{table_format.value.lower()}') if p.is_dir() else [path]

    operation_uuid = str(uuid.uuid4())
    with blobs_registries(abs_client, abs_container_name) as (blobs, metablobs):
        for filename in files:
            upload_file(
                filename,  # type: ignore
                p,
                abs_client,
                abs_container_name,
                operation_uuid=operation_uuid,
                blobs=blobs,
                metablobs=metablobs,
                max_concurrency=max_concurrency,
                force=force,
            )

        if not append:
            drop_table(table, client=client, force=True).wait()
        create_table(table, client=client).wait()

        common_blob_path = str(os.path.commonpath(blobs))
        include_files_pattern = len(blobs) > 1
        copy_into_table(
            client=client,
            table=table,
            table_format=table_format,
            blob_path=common_blob_path,
            include_files_pattern=include_files_pattern,
            abs_name=abs_name,
            abs_container_name=abs_container_name,
        )


def merge_table(
    table: 'Union[str , Table]',
    new_records: 'Union[Iterator[Dict] , List[Dict]]',
    partition_by: 'Union[str , List[str]]',
    client: 'DbxIOClient',
) -> None:
    """
    Merge new data into table. Use this function only if you have partitioning. Without partitioning, it's the same as
    append write operation.
    Function always waits till the end of deleting tmp table
    """
    dbxio_table = Table.from_obj(table)
    tmp_table = Table(table_identifier=f'{dbxio_table.table_identifier}__dbxio_tmp', schema=dbxio_table.schema)

    write_table(tmp_table, new_records, client=client, append=False).wait()

    _destination_alias = 'DBXIO_DESTINATION'
    _source_alias = 'DBXIO_SOURCE'
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    partition_by_sql_statement = ' AND '.join(
        [f'{_source_alias}.{part_col} == {_destination_alias}.{part_col}' for part_col in partition_by]
    )

    merge_sql_query = dedent(f"""
        MERGE INTO {dbxio_table.safe_table_identifier} AS {_destination_alias}
        USING {tmp_table.safe_table_identifier} AS {_source_alias}
        ON {partition_by_sql_statement}

        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
    try:
        client.sql(merge_sql_query).wait()
    finally:
        drop_table(tmp_table, client=client, force=True).wait()


def set_comment_on_table(
    table: 'Union[str , Table]',
    comment: Union[str, None],
    client: 'DbxIOClient',
) -> _FutureBaseResult:
    """
    Sets a comment on a table.
    Description comment supports Markdown.

    If the comment is None, the comment will be removed.
    """
    dbxio_table = Table.from_obj(table)
    set_comment_query = dedent(
        f"""COMMENT ON TABLE {dbxio_table.safe_table_identifier} IS {f'"{comment}"' if comment else 'NULL'}"""
    )

    return client.sql(set_comment_query)


def unset_comment_on_table(table: 'Union[str , Table]', client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets the comment on a table.
    """
    return set_comment_on_table(table=table, comment=None, client=client)


def get_comment_on_table(table: 'Union[str , Table]', client: 'DbxIOClient') -> Union[str, None]:
    """
    Returns the comment on a table.
    """
    dbxio_table = Table.from_obj(table)
    table_catalog, table_schema, table_name = dbxio_table.table_identifier.split('.')
    information_schema_query = dedent(f"""
    select comment
    from system.information_schema.tables
    where
        table_catalog = '{table_catalog}'
        and table_schema = '{table_schema}'
        and table_name = '{table_name}'
    """)

    with client.sql(information_schema_query) as result:
        for row in result:
            return row['comment']

    return None


def set_tags_on_table(table: 'Union[str , Table]', tags: dict[str, str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Sets tags on a table.
    Each tag is a key-value pair of strings.
    """
    assert tags, 'Tags must be a non-empty dictionary.'

    dbxio_table = Table.from_obj(table)
    set_tags_query = dedent(f"""
    ALTER TABLE {dbxio_table.safe_table_identifier}
    SET TAGS ({', '.join([f'"{k}" = "{v}"' for k, v in tags.items()])})
    """)

    return client.sql(set_tags_query)


def unset_tags_on_table(table: 'Union[str , Table]', tags: list[str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets tags on a table.
    """
    assert tags, 'Tags must be a non-empty list.'

    dbxio_table = Table.from_obj(table)
    unset_tags_query = dedent(f"""
    ALTER TABLE {dbxio_table.safe_table_identifier}
    UNSET TAGS ({', '.join([f'"{tag}"' for tag in tags])})
    """)

    return client.sql(unset_tags_query)


def get_tags_on_table(table: 'Union[str , Table]', client: 'DbxIOClient') -> dict[str, str]:
    """
    Returns the tags on a table.
    """
    dbxio_table = Table.from_obj(table)
    catalog_name, schema_name, table_name = dbxio_table.table_identifier.split('.')
    information_schema_query = dedent(f"""
    select tag_name, tag_value
    from system.information_schema.table_tags
    where
        catalog_name = '{catalog_name}'
        and schema_name = '{schema_name}'
        and table_name = '{table_name}'
    """)

    tags = {}
    with client.sql(information_schema_query) as result:
        for row in result:
            tags[row['tag_name']] = row['tag_value']

    return tags
