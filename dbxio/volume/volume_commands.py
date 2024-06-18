import os
import uuid
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Union

import attrs
from databricks.sdk.service.catalog import VolumeType
from tenacity import retry, stop_after_attempt, wait_fixed

from dbxio.blobs.block_upload import upload_file
from dbxio.blobs.download import download_blob_tree
from dbxio.core.cloud.client.object_storage import ObjectStorageClient
from dbxio.sql.results import _FutureBaseResult
from dbxio.utils.blobs import blobs_registries
from dbxio.utils.logging import get_logger

if TYPE_CHECKING:
    from dbxio.core.client import DbxIOClient

DEFAULT_EXTERNAL_LOCATION = 'default_external_location'

logger = get_logger()


@attrs.define(slots=True)
class Volume:
    """
    Represents a volume in Databricks.

    Storage_location is required for external volumes.
    It must satisfy the following pattern:
        abfss://<container_name>@<storage_name>.dfs.core.windows.net/<blobs_path>}
    """

    catalog: str = attrs.field(validator=attrs.validators.instance_of(str))
    schema: str = attrs.field(validator=attrs.validators.instance_of(str))
    name: str = attrs.field(validator=attrs.validators.instance_of(str))
    path: str = attrs.field(default='', validator=attrs.validators.instance_of(str))
    volume_type: VolumeType = attrs.field(
        default=VolumeType.MANAGED,
        validator=attrs.validators.instance_of(VolumeType),
    )
    storage_location: Union[str, None] = None

    def __attrs_post_init__(self):
        if self.volume_type == VolumeType.EXTERNAL and not self.storage_location:
            raise ValueError('storage_location is required for external volumes')

    def __truediv__(self, path: str):
        return Volume(
            catalog=self.catalog,
            schema=self.schema,
            name=self.name,
            path=str(Path(self.path) / Path(path)),
            volume_type=self.volume_type,
            storage_location=self.storage_location,
        )

    @property
    def full_name(self):
        return f'{self.catalog}.{self.schema}.{self.name}'

    @property
    def safe_full_name(self):
        return f'`{self.catalog}`.`{self.schema}`.`{self.name}`'

    @property
    def mount_path(self):
        return str(Path(f'/Volumes/{self.catalog}/{self.schema}/{self.name}') / self.path)

    def is_managed(self):
        return self.volume_type is VolumeType.MANAGED

    def is_external(self):
        return self.volume_type is VolumeType.EXTERNAL


def create_volume(volume: Volume, client: 'DbxIOClient') -> None:
    client.workspace_api.volumes.create(
        catalog_name=volume.catalog,
        schema_name=volume.schema,
        name=volume.name,
        volume_type=volume.volume_type,
        storage_location=volume.storage_location,
    )
    logger.info(f'Volume {volume.safe_full_name} was successfully created.')


def _exists_volume(volume: Volume, client: 'DbxIOClient') -> bool:
    for v in client.workspace_api.volumes.list(catalog_name=volume.catalog, schema_name=volume.schema):
        if v.name == volume.name:
            return True

    return False


def _download_external_volume(local_path: Path, storage_location: str):
    object_storage = ObjectStorageClient.from_url(storage_location)
    download_blob_tree(
        object_storage_client=object_storage,
        local_path=local_path,
        prefix_path=object_storage.blobs_path,
    )


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def _download_single_file_from_managed_volume(local_path: Path, file_path: str, client: 'DbxIOClient'):
    with open(local_path / Path(file_path).name, 'wb') as f:
        response_content = client.workspace_api.files.download(file_path).contents
        if response_content:
            f.write(response_content.read())
        else:
            raise ValueError(f'Failed to download file {file_path}, got None')


def _download_managed_volume(local_path: Path, volume: Volume, client: 'DbxIOClient'):
    for file in client.workspace_api.files.list_directory_contents(volume.mount_path):
        if file.name is None or file.path is None:
            raise ValueError(f'File {file} has no name or path')

        if file.is_directory:
            (local_path / file.name).mkdir(parents=True, exist_ok=True)
            _download_managed_volume(local_path / file.name, volume / file.name, client)
        else:
            _download_single_file_from_managed_volume(local_path, file.path, client)


def download_volume(
    path: Union[str, Path],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    client: 'DbxIOClient',
) -> Path:
    """
    Downloads files from a volume in Databricks to a local directory.
    Volume must be an external volume.

    :param path: local directory to download files to. Must be a directory.
    :param catalog_name: databricks catalog name
    :param schema_name: databricks schema name
    :param volume_name: volume name
    :param client: dbxio client
    """
    path = Path(path)
    if not path.exists() or not path.is_dir():
        raise ValueError('Path must be an existing directory')
    volume_info = client.workspace_api.volumes.read(f'{catalog_name}.{schema_name}.{volume_name}')
    if volume_info.volume_type is None:
        raise ValueError(f'Volume {catalog_name}.{schema_name}.{volume_name} does not exist')

    volume = Volume(
        catalog=catalog_name,
        schema=schema_name,
        name=volume_name,
        volume_type=volume_info.volume_type,
        storage_location=volume_info.storage_location,
    )
    logger.info(f'Downloading volume: {volume.full_name}')
    logger.debug(f'Volume type: {volume.volume_type}')

    if volume_info.volume_type == VolumeType.EXTERNAL:
        assert volume_info.storage_location, f'External volume must have a storage location, got {volume_info=}'
        _download_external_volume(local_path=path, storage_location=volume_info.storage_location)
    elif volume_info.volume_type == VolumeType.MANAGED:
        _download_managed_volume(local_path=path, volume=volume, client=client)
    else:
        raise ValueError(f'Unknown volume type: {volume_info.volume_type}')

    logger.info(f'Volume {volume.full_name} was successfully downloaded to {path}')

    return path


def write_volume(
    path: Union[str, Path],
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    client: 'DbxIOClient',
    create_volume_if_not_exists: bool = True,
    max_concurrency: int = 1,
    force: bool = False,
) -> None:
    """
    writes the contents of a local directory to a volume in Databricks.

    :param path: local directory or file to upload
    :param catalog_name: databricks catalog name
    :param schema_name: databricks schema name
    :param volume_name: volume name
    :param client: dbxio client
    :param create_volume_if_not_exists: flag to create the volume if it does not exist
    :param max_concurrency: the maximum number of threads to use for uploading files
    :param force: a flag to break leases if the blob is already in use
    """
    path = Path(path)

    catalog = client.workspace_api.catalogs.get(catalog_name)
    catalog_properties = catalog.properties if catalog.properties else {}

    default_external_location = catalog_properties.get(DEFAULT_EXTERNAL_LOCATION)
    if not default_external_location:
        raise ValueError(
            'Catalog does not have a default external location (aka container in storage account). '
            'Ask data engineers for creation of a default external location for your catalog.'
        )
    storage_url = client.workspace_api.external_locations.get(default_external_location).url
    assert storage_url, f'External location {default_external_location} does not have a URL'
    object_storage_client = ObjectStorageClient.from_url(storage_url)

    operation_uuid = str(uuid.uuid4())
    with blobs_registries(object_storage_client, keep_blobs=True) as (blobs, metablobs):
        # here all files in the path, including subdirectories, are uploaded to the blob storage.
        # only "hidden" files (those starting with a dot) are skipped
        for file in path.glob('**/*'):
            if file.is_file() and not file.name.startswith('.'):
                upload_file(
                    path=file,
                    local_path=path,
                    object_storage_client=object_storage_client,
                    operation_uuid=operation_uuid,
                    blobs=blobs,
                    metablobs=metablobs,
                    max_concurrency=max_concurrency,
                    force=force,
                )
        object_storage_client.blobs_path = str(os.path.commonpath(blobs))
        storage_location = object_storage_client.to_url()
    volume = Volume(
        catalog=catalog_name,
        schema=schema_name,
        name=volume_name,
        volume_type=VolumeType.EXTERNAL,
        storage_location=storage_location,
    )

    if not _exists_volume(volume, client):
        if create_volume_if_not_exists:
            create_volume(volume=volume, client=client)
        else:
            raise ValueError(
                f'Volume {volume_name} does not exist in schema {schema_name} of catalog '
                f'{catalog_name} and create_volume_if_not_exists is False.'
            )


def set_tags_on_volume(volume: Volume, tags: dict[str, str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Sets tags on a volume.
    Each tag is a key-value pair of strings.
    """
    if not tags:
        raise ValueError('tags must be a non-empty dictionary')

    set_tags_query = dedent(f"""
    ALTER VOLUME {volume.safe_full_name}
    SET TAGS ({', '.join([f'"{k}" = "{v}"' for k, v in tags.items()])})
    """)

    return client.sql(set_tags_query)


def unset_tags_on_volume(volume: Volume, tags: list[str], client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets tags on a volume.
    """
    if not tags:
        raise ValueError('tags must be a non-empty list')

    unset_tags_query = dedent(f"""
    ALTER VOLUME {volume.safe_full_name}
    UNSET TAGS ({', '.join([f'"{tag}"' for tag in tags])})
    """)

    return client.sql(unset_tags_query)


def get_tags_on_volume(volume: Volume, client: 'DbxIOClient') -> dict[str, str]:
    """
    Returns the tags on a volume.
    """
    information_schema_query = dedent(f"""
    select tag_name, tag_value
    from system.information_schema.volume_tags
    where
        catalog_name = '{volume.catalog}'
        and schema_name = '{volume.schema}'
        and volume_name = '{volume.name}'
    """)

    tags = {}
    with client.sql(information_schema_query) as result:
        for row in result:
            tags[row['tag_name']] = row['tag_value']

    return tags


def set_comment_on_volume(
    volume: Volume,
    comment: Union[str, None],
    client: 'DbxIOClient',
) -> _FutureBaseResult:
    """
    Sets a comment on a volume.
    Description comment supports Markdown.

    If the comment is None, the comment will be removed.
    """
    set_comment_query = dedent(
        f"""COMMENT ON VOLUME {volume.safe_full_name} IS {f'"{comment}"' if comment else 'NULL'}"""
    )

    return client.sql(set_comment_query)


def unset_comment_on_volume(volume: Volume, client: 'DbxIOClient') -> _FutureBaseResult:
    """
    Unsets the comment on a volume.
    """
    return set_comment_on_volume(volume=volume, comment=None, client=client)


def get_comment_on_volume(volume: Volume, client: 'DbxIOClient') -> Union[str, None]:
    """
    Returns the comment on a volume.
    """
    information_schema_query = dedent(f"""
    select comment
    from system.information_schema.volumes
    where
        catalog_name = '{volume.catalog}'
        and schema_name = '{volume.schema}'
        and volume_name = '{volume.name}'
    """)

    with client.sql(information_schema_query) as result:
        for row in result:
            return row['comment']

    return None
